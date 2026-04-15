package com.backend.models.teacher;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TeacherRepository extends JpaRepository<TeacherEntity, Long> {

    boolean existsByIdAndSpecializations_Id(Long id, Long subjectId);

    List<TeacherEntity> findDistinctByDepartment_IdAndSpecializations_Id(Long departmentId, Long subjectId);

    List<TeacherEntity> findDistinctBySpecializations_Id(Long subjectId);

    List<TeacherEntity> findByDepartment_Id(Long departmentId);
}
