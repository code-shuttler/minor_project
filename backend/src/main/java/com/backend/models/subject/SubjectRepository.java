package com.backend.models.subject;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SubjectRepository extends JpaRepository<SubjectEntity, Long> {
    boolean existsByNameIgnoreCase(String name);

    Optional<SubjectEntity> findByNameIgnoreCase(String name);
}
