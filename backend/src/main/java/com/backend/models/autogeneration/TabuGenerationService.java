package com.backend.models.autogeneration;

import java.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backend.exceptions.InvalidRequestException;
import com.backend.exceptions.TimetableGenerationTimeoutException;
import com.backend.models.batch.BatchEntity;
import com.backend.models.batch.BatchRepository;
import com.backend.models.room.RoomEntity;
import com.backend.models.room.RoomRepository;
import com.backend.models.subject.SubjectEntity;
import com.backend.models.teacher.TeacherEntity;
import com.backend.models.teacher.TeacherRepository;
import com.backend.models.timeslots.TimeSlotEntity;
import com.backend.models.timeslots.TimeSlotRepository;
import com.backend.models.timetable.TimetableAutoGenerateRequestModel;
import com.backend.models.timetable.TimetableEntity;
import com.backend.models.timetable.TimetableRepository;
import com.backend.models.timetable.TimetableResponseModel;

/**
 * Tabu Search based timetable generation service.
 *
 * How it works:
 * 1. Build a list of all sessions that need to be scheduled (lectures, tutorials, practicals).
 * 2. Create an initial solution by randomly assigning each session to a slot/teacher/room.
 * 3. Evaluate the solution using a conflict count (hard constraints violations).
 * 4. Iteratively try swapping sessions to reduce conflicts.
 * 5. Keep a tabu list of recently tried moves to avoid cycling.
 * 6. Stop when zero conflicts found or max iterations reached.
 */
@Service
public class TabuGenerationService {

    private static final Logger log = LoggerFactory.getLogger(TabuGenerationService.class);

    // ── Tabu Search parameters ──────────────────────────────────────────────
    private static final int MAX_ITERATIONS      = 5_000;
    private static final int TABU_TENURE         = 20;   // how long a move stays tabu
    private static final int NEIGHBOR_SAMPLE     = 40;   // neighbors evaluated per iteration
    private static final int MAX_RESTARTS        = 5;    // random restarts if stuck

    @Autowired private BatchRepository      batchRepository;
    @Autowired private TimeSlotRepository   timeSlotRepository;
    @Autowired private TeacherRepository    teacherRepository;
    @Autowired private TimetableRepository  timetableRepository;
    @Autowired private RoomRepository       roomRepository;

    @Value("${app.timetable.max-generation-ms:1800000}")
    private long maxGenerationMs;

    // ── Public entry point ──────────────────────────────────────────────────

    @Transactional
    public List<TimetableResponseModel> generateTimetable(TimetableAutoGenerateRequestModel request) {

        long deadline = System.currentTimeMillis() + maxGenerationMs;

        List<BatchEntity>    batches = batchRepository.findAllById(request.getBatchIds());
        List<TimeSlotEntity> slots   = timeSlotRepository.findAll();
        List<RoomEntity>     rooms   = roomRepository.findAll();

        if (batches.isEmpty() || slots.isEmpty() || rooms.isEmpty()) {
            throw new InvalidRequestException("Missing required data: batches, slots or rooms are empty.");
        }

        // Force-load lazy subjects inside the transaction
        batches.forEach(b -> b.getSubject().size());

        // Build the flat list of sessions that must be scheduled
        List<Session> sessions = buildSessions(batches);
        if (sessions.isEmpty()) {
            throw new InvalidRequestException("No sessions to schedule. Check subject lecture/tutorial/practical counts.");
        }

        // Delete existing timetable entries for these batches BEFORE generating
        timetableRepository.deleteByBatch_IdIn(request.getBatchIds());
        timetableRepository.flush();

        // Load existing entries for OTHER batches to avoid conflicts with them
        List<TimetableEntity> existingOthers = timetableRepository.findAll();

        // Pre-cache teachers per subject to avoid repeated DB calls
        Map<Long, List<TeacherEntity>> teacherCache = buildTeacherCache(sessions);

        List<TimetableEntity> best = null;
        int bestConflictsSoFar = Integer.MAX_VALUE;

        for (int restart = 0; restart < MAX_RESTARTS; restart++) {
            if (System.currentTimeMillis() > deadline) break;
            log.info("[TabuSearch] Starting restart {}/{}", restart + 1, MAX_RESTARTS);
            List<TimetableEntity> result = tabuSearch(sessions, slots, rooms, teacherCache, existingOthers, deadline);
            if (result != null) {
                int c = countConflicts(result, existingOthers);
                if (c < bestConflictsSoFar) {
                    bestConflictsSoFar = c;
                    best = result;
                }
                if (bestConflictsSoFar == 0) {
                    log.info("[TabuSearch] Zero conflicts found on restart {}. Done.", restart + 1);
                    break;
                }
            }
        }

        if (best == null || countConflicts(best, existingOthers) > 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new TimetableGenerationTimeoutException(
                    "Tabu search timed out. Try reducing the number of sessions or increasing available slots.");
            }
            throw new InvalidRequestException(
                "Tabu search could not find a conflict-free timetable. " +
                "Try adding more time slots, rooms, or teachers.");
        }

        try {
            return timetableRepository.saveAllAndFlush(best)
                    .stream()
                    .map(this::toResponse)
                    .toList();
        } catch (DataIntegrityViolationException ex) {
            throw new InvalidRequestException(
                "Timetable conflict detected during save. Please retry.");
        }
    }

    // ── Core Tabu Search ────────────────────────────────────────────────────

    private List<TimetableEntity> tabuSearch(
            List<Session> sessions,
            List<TimeSlotEntity> slots,
            List<RoomEntity> rooms,
            Map<Long, List<TeacherEntity>> teacherCache,
            List<TimetableEntity> existingOthers,
            long deadline) {

        Random rng = new Random();

        // Initial random solution
        List<TimetableEntity> current = buildRandomSolution(sessions, slots, rooms, teacherCache, rng);
        List<TimetableEntity> bestSolution = deepCopy(current);
        int bestConflicts = countConflicts(current, existingOthers);

        // Tabu list: stores string keys of recently used moves
        Deque<String> tabuList = new ArrayDeque<>();
        Set<String>   tabuSet  = new HashSet<>();

        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            if (System.currentTimeMillis() > deadline) break;
            if (bestConflicts == 0) break;

            TimetableEntity bestNeighborEntity  = null;
            TimetableEntity bestNeighborReplacement = null;
            int             bestNeighborConflicts   = Integer.MAX_VALUE;
            String          bestMoveKey             = null;

            // Sample random neighbors by trying to reassign a random session
            for (int n = 0; n < NEIGHBOR_SAMPLE; n++) {
                int idx = rng.nextInt(current.size());
                TimetableEntity entry = current.get(idx);

                TimeSlotEntity newSlot  = slots.get(rng.nextInt(slots.size()));
                RoomEntity     newRoom  = rooms.get(rng.nextInt(rooms.size()));
                List<TeacherEntity> teachers = teacherCache.getOrDefault(
                        entry.getSubject().getId(), List.of());
                if (teachers.isEmpty()) continue;
                TeacherEntity newTeacher = teachers.get(rng.nextInt(teachers.size()));

                String moveKey = buildMoveKey(idx, newSlot, newRoom, newTeacher);

                // Apply move temporarily
                TimetableEntity original = cloneEntry(entry);
                entry.setTimeSlot(newSlot);
                entry.setRoom(newRoom);
                entry.setTeacher(newTeacher);

                int conflicts = countConflicts(current, existingOthers);

                // Accept if not tabu OR if it beats the best known solution (aspiration)
                if (!tabuSet.contains(moveKey) || conflicts < bestConflicts) {
                    if (conflicts < bestNeighborConflicts) {
                        bestNeighborConflicts    = conflicts;
                        bestNeighborEntity       = entry;
                        bestNeighborReplacement  = cloneEntry(entry);
                        bestMoveKey              = moveKey;
                    }
                }

                // Rollback
                entry.setTimeSlot(original.getTimeSlot());
                entry.setRoom(original.getRoom());
                entry.setTeacher(original.getTeacher());
            }

            if (bestNeighborEntity == null) continue;

            // Apply best neighbor move
            bestNeighborEntity.setTimeSlot(bestNeighborReplacement.getTimeSlot());
            bestNeighborEntity.setRoom(bestNeighborReplacement.getRoom());
            bestNeighborEntity.setTeacher(bestNeighborReplacement.getTeacher());

            // Update tabu list
            tabuList.addLast(bestMoveKey);
            tabuSet.add(bestMoveKey);
            if (tabuList.size() > TABU_TENURE) {
                String removed = tabuList.removeFirst();
                tabuSet.remove(removed);
            }

            int currentConflicts = countConflicts(current, existingOthers);
            if (currentConflicts < bestConflicts) {
                bestConflicts  = currentConflicts;
                bestSolution   = deepCopy(current);
                log.info("[TabuSearch] iter={} conflicts={}", iter, bestConflicts);
            }
        }

        return bestSolution;
    }

    // ── Conflict counting ───────────────────────────────────────────────────

    /**
     * Counts hard constraint violations:
     * - Same batch in same slot more than once
     * - Same teacher in same slot more than once
     * - Same room in same slot more than once
     */
    private int countConflicts(List<TimetableEntity> timetable, List<TimetableEntity> existing) {
        Map<String, Integer> batchSlot   = new HashMap<>();
        Map<String, Integer> teacherSlot = new HashMap<>();
        Map<String, Integer> roomSlot    = new HashMap<>();

        // Seed with existing other-batch entries
        for (TimetableEntity e : existing) {
            String slotId = String.valueOf(e.getTimeSlot().getId());
            teacherSlot.merge(e.getTeacher().getId() + "_" + slotId, 1, Integer::sum);
            roomSlot.merge(e.getRoom().getId()        + "_" + slotId, 1, Integer::sum);
        }

        for (TimetableEntity e : timetable) {
            String slotId = String.valueOf(e.getTimeSlot().getId());
            batchSlot.merge(e.getBatch().getId()   + "_" + slotId, 1, Integer::sum);
            teacherSlot.merge(e.getTeacher().getId() + "_" + slotId, 1, Integer::sum);
            roomSlot.merge(e.getRoom().getId()       + "_" + slotId, 1, Integer::sum);
        }

        int conflicts = 0;
        for (int v : batchSlot.values())   if (v > 1) conflicts += (v - 1);
        for (int v : teacherSlot.values()) if (v > 1) conflicts += (v - 1);
        for (int v : roomSlot.values())    if (v > 1) conflicts += (v - 1);
        return conflicts;
    }

    // ── Session building ────────────────────────────────────────────────────

    /**
     * A Session represents one class that needs to be scheduled.
     * e.g. Batch A, Subject Math, type LECTURE
     */
    private record Session(BatchEntity batch, SubjectEntity subject) {}

    private List<Session> buildSessions(List<BatchEntity> batches) {
        List<Session> sessions = new ArrayList<>();
        for (BatchEntity batch : batches) {
            for (SubjectEntity subject : batch.getSubject()) {
                int total = safe(subject.getLecture())
                          + safe(subject.getTutorial())
                          + safe(subject.getPractical());
                for (int i = 0; i < Math.max(total, 1); i++) {
                    sessions.add(new Session(batch, subject));
                }
            }
        }
        return sessions;
    }

    private int safe(Integer v) { return v == null ? 0 : Math.max(v, 0); }

    // ── Initial solution ────────────────────────────────────────────────────

    private List<TimetableEntity> buildRandomSolution(
            List<Session> sessions,
            List<TimeSlotEntity> slots,
            List<RoomEntity> rooms,
            Map<Long, List<TeacherEntity>> teacherCache,
            Random rng) {

        List<TimetableEntity> solution = new ArrayList<>();
        for (Session session : sessions) {
            List<TeacherEntity> teachers = teacherCache.getOrDefault(
                    session.subject().getId(), List.of());
            if (teachers.isEmpty()) continue;

            TimetableEntity entry = new TimetableEntity();
            entry.setBatch(session.batch());
            entry.setSubject(session.subject());
            entry.setTeacher(teachers.get(rng.nextInt(teachers.size())));
            entry.setTimeSlot(slots.get(rng.nextInt(slots.size())));
            entry.setRoom(rooms.get(rng.nextInt(rooms.size())));
            solution.add(entry);
        }
        return solution;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<TeacherEntity> getTeachersForSubject(SubjectEntity subject) {
        return teacherRepository.findDistinctBySpecializations_Id(subject.getId());
    }

    private Map<Long, List<TeacherEntity>> buildTeacherCache(List<Session> sessions) {
        Map<Long, List<TeacherEntity>> cache = new HashMap<>();
        for (Session session : sessions) {
            Long subjectId = session.subject().getId();
            if (subjectId == null) continue;
            cache.computeIfAbsent(subjectId,
                    id -> teacherRepository.findDistinctBySpecializations_Id(id));
        }
        return cache;
    }

    private String buildMoveKey(int entryIndex, TimeSlotEntity slot, RoomEntity room, TeacherEntity teacher) {
        return entryIndex + "_" + slot.getId() + "_" + room.getId() + "_" + teacher.getId();
    }

    private TimetableEntity cloneEntry(TimetableEntity e) {
        TimetableEntity clone = new TimetableEntity();
        clone.setBatch(e.getBatch());
        clone.setSubject(e.getSubject());
        clone.setTeacher(e.getTeacher());
        clone.setTimeSlot(e.getTimeSlot());
        clone.setRoom(e.getRoom());
        return clone;
    }

    private List<TimetableEntity> deepCopy(List<TimetableEntity> list) {
        List<TimetableEntity> copy = new ArrayList<>(list.size());
        for (TimetableEntity e : list) copy.add(cloneEntry(e));
        return copy;
    }

    private TimetableResponseModel toResponse(TimetableEntity e) {
        return new TimetableResponseModel(
                e.getId(),
                e.getBatch().getId(),
                e.getSubject().getId(),
                e.getTeacher().getId(),
                e.getTimeSlot().getId(),
                e.getRoom().getId());
    }
}
