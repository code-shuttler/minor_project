package com.backend.models.timeslots;

import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.backend.exceptions.DuplicateResourceException;
import com.backend.exceptions.InvalidRequestException;
import com.backend.exceptions.ResourceNotFoundException;

@Service
public class TimeSlotService {

    private static final Set<String> ALLOWED_DAYS = Set.of(
            "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY");

    @Autowired
    private TimeSlotRepository timeSlotRepository;

    public List<TimeSlotResponseModel> getAllTimeSlots() {
        return timeSlotRepository.findAll().stream().map(this::toResponse).toList();
    }

    public TimeSlotResponseModel getTimeSlotById(Long id) {
        TimeSlotEntity entity = timeSlotRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Time slot not found with id: " + id));
        return toResponse(entity);
    }

    public TimeSlotResponseModel createTimeSlot(TimeSlotRequestModel request) {
        String normalizedDay = normalizeDay(request.getDay());
        validateTimeRange(request.getStartTime(), request.getEndTime());
        validateUnique(normalizedDay, request.getStartTime(), request.getEndTime(), null);

        TimeSlotEntity entity = new TimeSlotEntity();
        entity.setDay(normalizedDay);
        entity.setStartTime(request.getStartTime());
        entity.setEndTime(request.getEndTime());

        try {
            TimeSlotEntity saved = timeSlotRepository.save(entity);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Time slot already exists for the same day and time range");
        }
    }

    public TimeSlotResponseModel updateTimeSlot(Long id, TimeSlotRequestModel request) {
        TimeSlotEntity existing = timeSlotRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Time slot not found with id: " + id));

        String normalizedDay = normalizeDay(request.getDay());
        validateTimeRange(request.getStartTime(), request.getEndTime());
        validateUnique(normalizedDay, request.getStartTime(), request.getEndTime(), id);

        existing.setDay(normalizedDay);
        existing.setStartTime(request.getStartTime());
        existing.setEndTime(request.getEndTime());

        try {
            TimeSlotEntity saved = timeSlotRepository.save(existing);
            return toResponse(saved);
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateResourceException("Time slot already exists for the same day and time range");
        }
    }

    public void deleteTimeSlot(Long id) {
        if (!timeSlotRepository.existsById(id)) {
            throw new ResourceNotFoundException("Time slot not found with id: " + id);
        }
        timeSlotRepository.deleteById(id);
    }

    private void validateTimeRange(LocalTime startTime, LocalTime endTime) {
        if (startTime != null && endTime != null && !startTime.isBefore(endTime)) {
            throw new InvalidRequestException("start_time must be earlier than end_time");
        }
    }

    private void validateUnique(String day, LocalTime startTime, LocalTime endTime, Long currentId) {
        timeSlotRepository.findByDayIgnoreCaseAndStartTimeAndEndTime(day, startTime, endTime).ifPresent(found -> {
            if (currentId == null || !found.getId().equals(currentId)) {
                throw new DuplicateResourceException("Time slot already exists for the same day and time range");
            }
        });
    }

    private String normalizeDay(String day) {
        if (day == null) {
            return null;
        }

        String normalized = day.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_DAYS.contains(normalized)) {
            throw new InvalidRequestException("Day must be a valid weekday");
        }
        return normalized;
    }

    private TimeSlotResponseModel toResponse(TimeSlotEntity entity) {
        return new TimeSlotResponseModel(
                entity.getId(),
                entity.getDay(),
                entity.getStartTime(),
                entity.getEndTime());
    }
}
