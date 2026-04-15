package com.backend.models.department;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DepartmentRepository extends JpaRepository<DepartmentEntity, Long> {
    boolean existsByNameIgnoreCase(String name);

    boolean existsByAbbreviationIgnoreCase(String abbreviation);

    Optional<DepartmentEntity> findByNameIgnoreCase(String name);

    Optional<DepartmentEntity> findByAbbreviationIgnoreCase(String abbreviation);
}
