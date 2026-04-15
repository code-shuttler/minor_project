package com.backend.models.batch;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface BatchRepository extends JpaRepository<BatchEntity, Long> {

    Optional<BatchEntity> findByDepartment_IdAndSemesterAndNameIgnoreCase(
            Long departmentId,
            Integer semester,
            String name);

    List<BatchEntity> findAllByOrderByIdAsc();

    boolean existsByIdAndSubject_Id(Long id, Long subjectId);
}
