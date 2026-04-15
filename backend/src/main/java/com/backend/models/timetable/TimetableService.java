package com.backend.models.timetable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.backend.exceptions.DuplicateResourceException;
import com.backend.exceptions.InvalidRequestException;
import com.backend.exceptions.ResourceNotFoundException;
import com.backend.models.batch.BatchEntity;
import com.backend.models.batch.BatchRepository;
import com.backend.models.room.RoomEntity;
import com.backend.models.room.RoomRepository;
import com.backend.models.subject.SubjectEntity;
import com.backend.models.subject.SubjectRepository;
import com.backend.models.teacher.TeacherEntity;
import com.backend.models.teacher.TeacherRepository;
import com.backend.models.timeslots.TimeSlotEntity;
import com.backend.models.timeslots.TimeSlotRepository;

@Service
public class TimetableService {

    private static final Map<String, Integer> DAY_SORT_ORDER = Map.of(
            "MONDAY", 1,
            "TUESDAY", 2,
            "WEDNESDAY", 3,
            "THURSDAY", 4,
            "FRIDAY", 5,
            "SATURDAY", 6,
            "SUNDAY", 7);

    @Autowired
    private TimetableRepository timetableRepository;

    @Autowired
    private BatchRepository batchRepository;

    @Autowired
    private SubjectRepository subjectRepository;

    @Autowired
    private TeacherRepository teacherRepository;

    @Autowired
    private TimeSlotRepository timeSlotRepository;

    @Autowired
    private RoomRepository roomRepository;

    public List<TimetableResponseModel> getAllTimetables() {
        return timetableRepository.findAll().stream().map(this::toResponse).toList();
    }

    public TimetableResponseModel getTimetableById(Long id) {
        TimetableEntity entity = timetableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Timetable entry not found with id: " + id));
        return toResponse(entity);
    }

    public TimetableResponseModel createTimetable(TimetableRequestModel request) {
        BatchEntity batch = getBatchOrThrow(request.getBatchId());
        SubjectEntity subject = getSubjectOrThrow(request.getSubjectId());
        TeacherEntity teacher = getTeacherOrThrow(request.getTeacherId());
        TimeSlotEntity timeSlot = getTimeSlotOrThrow(request.getTimeSlotId());
        RoomEntity room = getRoomOrThrow(request.getRoomId());

        validateBatchSubject(batch.getId(), subject.getId());
        validateTeacherSpecialization(teacher.getId(), subject.getId());
        validateBatchTimeSlotConflict(batch.getId(), timeSlot.getId(), null);
        validateTeacherTimeSlotConflict(teacher.getId(), timeSlot.getId(), null);
        validateRoomTimeSlotConflict(room.getId(), timeSlot.getId(), null);

        TimetableEntity entity = new TimetableEntity();
        entity.setBatch(batch);
        entity.setSubject(subject);
        entity.setTeacher(teacher);
        entity.setTimeSlot(timeSlot);
        entity.setRoom(room);

        try {
            TimetableEntity saved = timetableRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException(
                    "Timetable conflict detected for the selected batch/teacher/room and time slot");
        }
    }

    public TimetableResponseModel updateTimetable(Long id, TimetableRequestModel request) {
        TimetableEntity existing = timetableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Timetable entry not found with id: " + id));

        BatchEntity batch = getBatchOrThrow(request.getBatchId());
        SubjectEntity subject = getSubjectOrThrow(request.getSubjectId());
        TeacherEntity teacher = getTeacherOrThrow(request.getTeacherId());
        TimeSlotEntity timeSlot = getTimeSlotOrThrow(request.getTimeSlotId());
        RoomEntity room = getRoomOrThrow(request.getRoomId());

        validateBatchSubject(batch.getId(), subject.getId());
        validateTeacherSpecialization(teacher.getId(), subject.getId());
        validateBatchTimeSlotConflict(batch.getId(), timeSlot.getId(), id);
        validateTeacherTimeSlotConflict(teacher.getId(), timeSlot.getId(), id);
        validateRoomTimeSlotConflict(room.getId(), timeSlot.getId(), id);

        existing.setBatch(batch);
        existing.setSubject(subject);
        existing.setTeacher(teacher);
        existing.setTimeSlot(timeSlot);
        existing.setRoom(room);

        try {
            TimetableEntity saved = timetableRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException(
                    "Timetable conflict detected for the selected batch/teacher/room and time slot");
        }
    }

    public void deleteTimetable(Long id) {
        if (!timetableRepository.existsById(id)) {
            throw new ResourceNotFoundException("Timetable entry not found with id: " + id);
        }
        timetableRepository.deleteById(id);
    }

    @Transactional
    public List<TimetableResponseModel> autoGenerateTimetable(Integer batchCount) {
        if (batchCount == null || batchCount < 1) {
            throw new InvalidRequestException("batch_count must be greater than or equal to 1");
        }

        List<BatchEntity> allBatches = batchRepository.findAllByOrderByIdAsc();
        if (allBatches.isEmpty()) {
            throw new InvalidRequestException("No batches available for timetable generation");
        }

        if (batchCount > allBatches.size()) {
            throw new InvalidRequestException(
                "Requested batch_count is " + batchCount
                    + " but only " + allBatches.size() + " batches are available");
        }

        List<TimeSlotEntity> sortedTimeSlots = sortTimeSlots(timeSlotRepository.findAll());
        if (sortedTimeSlots.isEmpty()) {
            throw new InvalidRequestException("No time slots available; create time slots before auto-generation");
        }

        List<RoomEntity> availableRooms = roomRepository.findAll().stream()
                .sorted(Comparator.comparing(RoomEntity::getId))
                .toList();
        if (availableRooms.isEmpty()) {
            throw new InvalidRequestException("No rooms available; create rooms before auto-generation");
        }

        List<BatchEntity> selectedBatches = allBatches.subList(0, batchCount);
        validateBatchSubjects(selectedBatches);

        Set<Long> selectedBatchIds = selectedBatches.stream()
                .map(BatchEntity::getId)
                .collect(Collectors.toSet());

        timetableRepository.deleteByBatch_IdIn(selectedBatchIds);
        timetableRepository.flush();

        List<TimetableEntity> existingEntries = timetableRepository.findAll();
        Map<Long, Set<Long>> usedBatchSlots = new HashMap<>();
        Map<Long, Set<Long>> usedTeacherSlots = new HashMap<>();
        Map<Long, Set<Long>> usedRoomSlots = new HashMap<>();
        Map<Long, Integer> teacherLoad = new HashMap<>();

        for (TimetableEntity existingEntry : existingEntries) {
            markSlotUsage(
                    existingEntry.getBatch().getId(),
                    existingEntry.getTeacher().getId(),
                    existingEntry.getRoom().getId(),
                    existingEntry.getTimeSlot().getId(),
                    usedBatchSlots,
                    usedTeacherSlots,
                    usedRoomSlots,
                    teacherLoad);
        }

        selectedBatchIds.forEach(batchId -> usedBatchSlots.computeIfAbsent(batchId, key -> new HashSet<>()));

        List<TimetableEntity> generatedEntries = new ArrayList<>();

        for (BatchEntity batch : selectedBatches) {
            List<SubjectEntity> sortedSubjects = batch.getSubject().stream()
                    .sorted(Comparator.comparing(SubjectEntity::getName).thenComparing(SubjectEntity::getId))
                    .toList();

            for (SubjectEntity subject : sortedSubjects) {
                List<TeacherEntity> eligibleTeachers = teacherRepository
                    .findDistinctByDepartment_IdAndSpecializations_Id(
                        batch.getDepartment().getId(),
                        subject.getId());

                if (eligibleTeachers.isEmpty()) {
                    throw new InvalidRequestException(
                            "No eligible teacher found for subject '" + subject.getName()
                                    + "' in batch '" + batch.getName() + "'");
                }

                int requiredSessions = calculateRequiredSessions(subject);
                for (int sessionIndex = 0; sessionIndex < requiredSessions; sessionIndex++) {
                    AssignmentOption assignment = findAssignment(
                            batch,
                            subject,
                            eligibleTeachers,
                            availableRooms,
                            sortedTimeSlots,
                            usedBatchSlots,
                            usedTeacherSlots,
                            usedRoomSlots,
                            teacherLoad);

                    if (assignment == null) {
                        throw new InvalidRequestException(
                                "Not enough available time periods to schedule subject '"
                                        + subject.getName() + "' for batch '" + batch.getName() + "'");
                    }

                    TimetableEntity generated = new TimetableEntity();
                    generated.setBatch(batch);
                    generated.setSubject(subject);
                    generated.setTeacher(assignment.teacher());
                    generated.setTimeSlot(assignment.timeSlot());
                    generated.setRoom(assignment.room());
                    generatedEntries.add(generated);

                    markSlotUsage(
                            batch.getId(),
                            assignment.teacher().getId(),
                            assignment.room().getId(),
                            assignment.timeSlot().getId(),
                            usedBatchSlots,
                            usedTeacherSlots,
                            usedRoomSlots,
                            teacherLoad);
                }
            }
        }

        try {
            return timetableRepository.saveAll(generatedEntries).stream().map(this::toResponse).toList();
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException(
                    "Unable to auto-generate timetable due to a slot conflict");
        }
    }

    private BatchEntity getBatchOrThrow(Long batchId) {
        return batchRepository.findById(batchId)
                .orElseThrow(() -> new ResourceNotFoundException("Batch not found with id: " + batchId));
    }

    private SubjectEntity getSubjectOrThrow(Long subjectId) {
        return subjectRepository.findById(subjectId)
                .orElseThrow(() -> new ResourceNotFoundException("Subject not found with id: " + subjectId));
    }

    private TeacherEntity getTeacherOrThrow(Long teacherId) {
        return teacherRepository.findById(teacherId)
                .orElseThrow(() -> new ResourceNotFoundException("Teacher not found with id: " + teacherId));
    }

    private TimeSlotEntity getTimeSlotOrThrow(Long timeSlotId) {
        return timeSlotRepository.findById(timeSlotId)
                .orElseThrow(() -> new ResourceNotFoundException("Time slot not found with id: " + timeSlotId));
    }

    private RoomEntity getRoomOrThrow(Long roomId) {
        return roomRepository.findById(roomId)
                .orElseThrow(() -> new ResourceNotFoundException("Room not found with id: " + roomId));
    }

    private void validateBatchSubject(Long batchId, Long subjectId) {
        if (!batchRepository.existsByIdAndSubject_Id(batchId, subjectId)) {
            throw new InvalidRequestException("Selected subject is not assigned to the selected batch");
        }
    }

    private void validateTeacherSpecialization(Long teacherId, Long subjectId) {
        if (!teacherRepository.existsByIdAndSpecializations_Id(teacherId, subjectId)) {
            throw new InvalidRequestException("Selected teacher is not specialized for the selected subject");
        }
    }

    private void validateBatchTimeSlotConflict(Long batchId, Long timeSlotId, Long currentTimetableId) {
        timetableRepository.findByBatch_IdAndTimeSlot_Id(batchId, timeSlotId).ifPresent(found -> {
            if (currentTimetableId == null || !found.getId().equals(currentTimetableId)) {
                throw new DuplicateResourceException("Batch already has a timetable entry for the selected time slot");
            }
        });
    }

    private void validateTeacherTimeSlotConflict(Long teacherId, Long timeSlotId, Long currentTimetableId) {
        timetableRepository.findByTeacher_IdAndTimeSlot_Id(teacherId, timeSlotId).ifPresent(found -> {
            if (currentTimetableId == null || !found.getId().equals(currentTimetableId)) {
                throw new DuplicateResourceException(
                        "Teacher already has a timetable entry for the selected time slot");
            }
        });
    }

    private void validateRoomTimeSlotConflict(Long roomId, Long timeSlotId, Long currentTimetableId) {
        timetableRepository.findByRoom_IdAndTimeSlot_Id(roomId, timeSlotId).ifPresent(found -> {
            if (currentTimetableId == null || !found.getId().equals(currentTimetableId)) {
                throw new DuplicateResourceException(
                        "Room already has a timetable entry for the selected time slot");
            }
        });
    }

    private List<TimeSlotEntity> sortTimeSlots(List<TimeSlotEntity> timeSlots) {
        return timeSlots.stream()
                .sorted(Comparator
                .comparingInt((TimeSlotEntity slot) -> DAY_SORT_ORDER
                    .getOrDefault(slot.getDay(), Integer.MAX_VALUE))
                        .thenComparing(TimeSlotEntity::getStartTime)
                        .thenComparing(TimeSlotEntity::getEndTime)
                        .thenComparing(TimeSlotEntity::getId))
                .toList();
    }

    private void validateBatchSubjects(List<BatchEntity> batches) {
        for (BatchEntity batch : batches) {
            if (batch.getSubject() == null || batch.getSubject().isEmpty()) {
                throw new InvalidRequestException(
                        "Batch '" + batch.getName() + "' has no assigned subjects for timetable generation");
            }
        }
    }

    private int calculateRequiredSessions(SubjectEntity subject) {
        int lectureCount = safeNonNegative(subject.getLecture());
        int tutorialCount = safeNonNegative(subject.getTutorial());
        int practicalCount = safeNonNegative(subject.getPractical());
        int total = lectureCount + tutorialCount + practicalCount;
        return total > 0 ? total : 1;
    }

    private int safeNonNegative(Integer value) {
        if (value == null) {
            return 0;
        }
        return Math.max(value, 0);
    }

    private AssignmentOption findAssignment(
            BatchEntity batch,
            SubjectEntity subject,
            List<TeacherEntity> eligibleTeachers,
            List<RoomEntity> availableRooms,
            List<TimeSlotEntity> sortedTimeSlots,
            Map<Long, Set<Long>> usedBatchSlots,
            Map<Long, Set<Long>> usedTeacherSlots,
            Map<Long, Set<Long>> usedRoomSlots,
            Map<Long, Integer> teacherLoad) {

        Long batchId = batch.getId();
        List<TeacherEntity> sortedTeachers = eligibleTeachers.stream()
                .sorted(Comparator
                        .comparingInt((TeacherEntity teacher) -> teacherLoad.getOrDefault(teacher.getId(), 0))
                        .thenComparing(TeacherEntity::getId))
                .toList();

        for (TimeSlotEntity timeSlot : sortedTimeSlots) {
            Long timeSlotId = timeSlot.getId();
            if (usedBatchSlots.getOrDefault(batchId, Set.of()).contains(timeSlotId)) {
                continue;
            }

            for (TeacherEntity teacher : sortedTeachers) {
                Long teacherId = teacher.getId();
                if (usedTeacherSlots.getOrDefault(teacherId, Set.of()).contains(timeSlotId)) {
                    continue;
                }
                for (RoomEntity room : availableRooms) {
                    Long roomId = room.getId();
                    if (usedRoomSlots.getOrDefault(roomId, Set.of()).contains(timeSlotId)) {
                        continue;
                    }
                    return new AssignmentOption(teacher, room, timeSlot);
                }
            }
        }

        return null;
    }

    private void markSlotUsage(
            Long batchId,
            Long teacherId,
            Long roomId,
            Long timeSlotId,
            Map<Long, Set<Long>> usedBatchSlots,
            Map<Long, Set<Long>> usedTeacherSlots,
            Map<Long, Set<Long>> usedRoomSlots,
            Map<Long, Integer> teacherLoad) {

        usedBatchSlots.computeIfAbsent(batchId, key -> new HashSet<>()).add(timeSlotId);
        usedTeacherSlots.computeIfAbsent(teacherId, key -> new HashSet<>()).add(timeSlotId);
        usedRoomSlots.computeIfAbsent(roomId, key -> new HashSet<>()).add(timeSlotId);
        teacherLoad.merge(teacherId, 1, Integer::sum);
    }

    private record AssignmentOption(TeacherEntity teacher, RoomEntity room, TimeSlotEntity timeSlot) {
    }

    private TimetableResponseModel toResponse(TimetableEntity entity) {
        return new TimetableResponseModel(
                entity.getId(),
                entity.getBatch().getId(),
                entity.getSubject().getId(),
                entity.getTeacher().getId(),
                entity.getTimeSlot().getId(),
                entity.getRoom().getId());
    }
}