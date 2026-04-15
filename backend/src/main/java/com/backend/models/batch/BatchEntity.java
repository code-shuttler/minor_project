package com.backend.models.batch;

import java.util.HashSet;
import java.util.Set;

import com.backend.models.department.DepartmentEntity;
import com.backend.models.subject.SubjectEntity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "batches", uniqueConstraints = {
        @UniqueConstraint(name = "  uk_batch_dept_sem_name", columnNames = { "department_id", "semester", "name" })
})
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class BatchEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 120)
    private String name;

    @Column(nullable = false)
    private Integer semester;

    @ManyToMany
    @JoinTable(name = "batch_subjects", joinColumns = @JoinColumn(name = "batches_id"), inverseJoinColumns = @JoinColumn(name = "subject_id"))
    private Set<SubjectEntity> subject = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "department_id", nullable = false, foreignKey = @ForeignKey(name = "fk_batch_department"))
    private DepartmentEntity department;
}
