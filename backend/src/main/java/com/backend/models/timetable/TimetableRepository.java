package com.backend.models.timetable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TimetableRepository extends JpaRepository<TimetableEntity, Long> {

    Optional<TimetableEntity> findByBatch_IdAndTimeSlot_Id(Long batchId, Long timeSlotId);

    Optional<TimetableEntity> findByTeacher_IdAndTimeSlot_Id(Long teacherId, Long timeSlotId);

    Optional<TimetableEntity> findByRoom_IdAndTimeSlot_Id(Long roomId, Long timeSlotId);

    void deleteByBatch_IdIn(Set<Long> batchIds);

    List<TimetableEntity> findByBatch_IdIn(Set<Long> batchIds);
}