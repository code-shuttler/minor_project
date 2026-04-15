package com.backend.models.timetable;

import com.backend.models.batch.BatchEntity;
import com.backend.models.room.RoomEntity;
import com.backend.models.subject.SubjectEntity;
import com.backend.models.teacher.TeacherEntity;
import com.backend.models.timeslots.TimeSlotEntity;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "timetables", uniqueConstraints = {
        @UniqueConstraint(name = "uk_timetable_batch_slot", columnNames = { "batch_id", "time_slot_id" }),
    @UniqueConstraint(name = "uk_timetable_teacher_slot", columnNames = { "teacher_id", "time_slot_id" }),
    @UniqueConstraint(name = "uk_timetable_room_slot", columnNames = { "room_id", "time_slot_id" })
})
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimetableEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "batch_id", nullable = false, foreignKey = @ForeignKey(name = "fk_timetable_batch"))
    private BatchEntity batch;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "subject_id", nullable = false, foreignKey = @ForeignKey(name = "fk_timetable_subject"))
    private SubjectEntity subject;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "teacher_id", nullable = false, foreignKey = @ForeignKey(name = "fk_timetable_teacher"))
    private TeacherEntity teacher;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "time_slot_id", nullable = false, foreignKey = @ForeignKey(name = "fk_timetable_time_slot"))
    private TimeSlotEntity timeSlot;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "room_id", nullable = false, foreignKey = @ForeignKey(name = "fk_timetable_room"))
    private RoomEntity room;
}