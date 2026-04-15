package com.backend.models.timeslots;

import java.time.LocalTime;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TimeSlotRepository extends JpaRepository<TimeSlotEntity, Long> {
    Optional<TimeSlotEntity> findByDayIgnoreCaseAndStartTimeAndEndTime(String day, LocalTime startTime, LocalTime endTime);
}
