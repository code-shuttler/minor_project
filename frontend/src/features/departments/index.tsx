'use client';

import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { useCallback, useEffect, useMemo, useState } from "react";
import {
    createDepartment,
    deleteDepartment,
    listDepartments,
    type Department,
    updateDepartment,
} from "./departmentApi";
import { Input } from "@/shared/components/ui/input";
import { Button } from "@/shared/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/components/ui/tabs";
import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { CheckCircle, AlertCircle, Plus } from "lucide-react";
import { createTeacher, deleteTeacher, listTeachers, updateTeacher, type Teacher } from "@/features/teacher/teacherApi";
import { listSubjects, type Subject } from "@/features/subject/subjectApi";
import { createBatch, deleteBatch, listBatches, updateBatch, type Batch } from "@/features/batches/batchApi";
import type { ApiError } from "@/shared/api/http";

type FormState = {
    name: string;
    abbreviation: string;
};

type TeacherFormState = {
    name: string;
    abbreviation: string;
    specializationIds: number[];
};

type BatchFormState = {
    name: string;
    semester: string;
    subjectIds: number[];
};

const initialTeacherFormState: TeacherFormState = {
    name: "",
    abbreviation: "",
    specializationIds: [],
};

const initialFormState: FormState = {
    name: "",
    abbreviation: "",
};

const initialBatchFormState: BatchFormState = {
    name: "",
    semester: "",
    subjectIds: [],
};

const resolveErrorMessage = (error: unknown): string => {
    if (error instanceof Error) {
        return error.message;
    }
    if (typeof error === "string") {
        return error;
    }
    return "Something went wrong while processing the department request.";
};

const resolveTeacherErrorMessage = (error: unknown): string => {
    const apiError = error as ApiError;
    if (typeof apiError?.data === "string") {
        return apiError.data;
    }

    if (apiError?.data && typeof apiError.data === "object" && "message" in (apiError.data as object)) {
        const serverMessage = (apiError.data as { message?: unknown }).message;
        if (typeof serverMessage === "string" && serverMessage.trim().length > 0) {
            return serverMessage;
        }
    }

    if (apiError?.message && apiError.message.trim().length > 0) {
        return apiError.message;
    }

    return "Something went wrong while processing the teacher request.";
};

const resolveBatchErrorMessage = (error: unknown): string => {
    const apiError = error as ApiError;
    if (typeof apiError?.data === "string") {
        return apiError.data;
    }

    if (apiError?.data && typeof apiError.data === "object" && "message" in (apiError.data as object)) {
        const serverMessage = (apiError.data as { message?: unknown }).message;
        if (typeof serverMessage === "string" && serverMessage.trim().length > 0) {
            return serverMessage;
        }
    }

    if (apiError?.message && apiError.message.trim().length > 0) {
        return apiError.message;
    }

    return "Something went wrong while processing the batch request.";
};

function Departments() {
    const [departments, setDepartments] = useState<Department[]>([]);
    const [selectedDepartmentId, setSelectedDepartmentId] = useState<number | null>(null);

    const [teachers, setTeachers] = useState<Teacher[]>([]);
    const [selectedTeacherId, setSelectedTeacherId] = useState<number | null>(null);
    const [subjects, setSubjects] = useState<Subject[]>([]);
    const [batches, setBatches] = useState<Batch[]>([]);
    const [selectedBatchId, setSelectedBatchId] = useState<number | null>(null);

    const [teacherFormState, setTeacherFormState] = useState<TeacherFormState>(initialTeacherFormState);
    const [formState, setFormState] = useState<FormState>(initialFormState);
    const [batchFormState, setBatchFormState] = useState<BatchFormState>(initialBatchFormState);

    const [isEditing, setIsEditing] = useState(false);
    const [isEditingTeacher, setIsEditingTeacher] = useState(false);
    const [isEditingBatch, setIsEditingBatch] = useState(false);

    const [isLoading, setIsLoading] = useState(false);
    const [isLoadingTeachers, setIsLoadingTeachers] = useState(false);
    const [isLoadingBatches, setIsLoadingBatches] = useState(false);
    const [isSavingDepartment, setIsSavingDepartment] = useState(false);
    const [isSavingTeacher, setIsSavingTeacher] = useState(false);
    const [isSavingBatch, setIsSavingBatch] = useState(false);
    const [isDeletingDepartment, setIsDeletingDepartment] = useState(false);
    const [deletingTeacherId, setDeletingTeacherId] = useState<number | null>(null);
    const [deletingBatchId, setDeletingBatchId] = useState<number | null>(null);
    const [teacherSearchQuery, setTeacherSearchQuery] = useState("");
    const [batchSearchQuery, setBatchSearchQuery] = useState("");
    const [error, setError] = useState<string | null>(null);
    const [successMessage, setSuccessMessage] = useState<string | null>(null);

    const selectedDepartment = useMemo(
        () => departments.find((department) => department.id === selectedDepartmentId) ?? null,
        [departments, selectedDepartmentId]
    );

    const teachersInSelectedDepartment = useMemo(
        () => teachers.filter((teacher) => teacher.departmentId === selectedDepartmentId),
        [teachers, selectedDepartmentId]
    );

    const batchesInSelectedDepartment = useMemo(
        () => batches.filter((batch) => batch.department_id === selectedDepartmentId),
        [batches, selectedDepartmentId]
    );

    const filteredTeachers = useMemo(() => {
        const normalizedQuery = teacherSearchQuery.trim().toLowerCase();
        if (!normalizedQuery) {
            return teachersInSelectedDepartment;
        }

        return teachersInSelectedDepartment.filter((teacher) =>
            teacher.name.toLowerCase().includes(normalizedQuery)
            || teacher.abbreviation.toLowerCase().includes(normalizedQuery)
        );
    }, [teachersInSelectedDepartment, teacherSearchQuery]);

    const filteredBatches = useMemo(() => {
        const normalizedQuery = batchSearchQuery.trim().toLowerCase();
        if (!normalizedQuery) {
            return batchesInSelectedDepartment;
        }

        return batchesInSelectedDepartment.filter((batch) =>
            batch.name.toLowerCase().includes(normalizedQuery)
            || String(batch.semester).includes(normalizedQuery)
        );
    }, [batchesInSelectedDepartment, batchSearchQuery]);

    const subjectNameById = useMemo(
        () => new Map(subjects.map((subject) => [subject.id, subject.name])),
        [subjects]
    );

    const selectedSpecializationNames = useMemo(
        () => teacherFormState.specializationIds
            .map((id) => subjectNameById.get(id))
            .filter((name): name is string => Boolean(name)),
        [teacherFormState.specializationIds, subjectNameById]
    );

    const selectedBatchSubjectNames = useMemo(
        () => batchFormState.subjectIds
            .map((id) => subjectNameById.get(id))
            .filter((name): name is string => Boolean(name)),
        [batchFormState.subjectIds, subjectNameById]
    );

    const loadTeachers = useCallback(async () => {
        setIsLoadingTeachers(true);
        setError(null);
        try {
            const response = await listTeachers();
            setTeachers(response);
            if (response.length > 0) {
                setSelectedTeacherId((current) => current ?? response[0].id);
            } else {
                setSelectedTeacherId(null);
                setIsEditingTeacher(false);
                setTeacherFormState(initialTeacherFormState);
            }
        } catch (err) {
            setError(resolveTeacherErrorMessage(err));
        } finally {
            setIsLoadingTeachers(false);
        }
    }, []);

    const loadBatches = useCallback(async () => {
        setIsLoadingBatches(true);
        setError(null);
        try {
            const response = await listBatches();
            setBatches(response);
            if (response.length > 0) {
                setSelectedBatchId((current) => current ?? response[0].id);
            } else {
                setSelectedBatchId(null);
                setIsEditingBatch(false);
                setBatchFormState(initialBatchFormState);
            }
        } catch (err) {
            setError(resolveBatchErrorMessage(err));
        } finally {
            setIsLoadingBatches(false);
        }
    }, []);

    useEffect(() => {
        if (selectedTeacherId === null) {
            return;
        }

        const teacher = teachers.find((t) => t.id === selectedTeacherId);
        if (!teacher) {
            return;
        }

        setIsEditingTeacher(true);
        setTeacherFormState({
            name: teacher.name,
            abbreviation: teacher.abbreviation,
            specializationIds: teacher.specializationIds,
        });
    }, [teachers, selectedTeacherId]);

    useEffect(() => {
        if (selectedBatchId === null) {
            return;
        }

        const batch = batches.find((b) => b.id === selectedBatchId);
        if (!batch) {
            return;
        }

        setIsEditingBatch(true);
        setBatchFormState({
            name: batch.name,
            semester: String(batch.semester),
            subjectIds: batch.subject_ids,
        });
    }, [batches, selectedBatchId]);

    const loadDepartments = useCallback(async () => {
        setIsLoading(true);
        setError(null);
        try {
            const response = await listDepartments();
            setDepartments(response);
            const subjectsResponse = await listSubjects();
            setSubjects(subjectsResponse);
            if (response.length > 0) {
                setSelectedDepartmentId((current) => current ?? response[0].id);
                await loadTeachers();
                await loadBatches();
            } else {
                setSelectedDepartmentId(null);
            }
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsLoading(false);
        }
    }, [loadTeachers, loadBatches]);

    useEffect(() => {
        void loadDepartments();
    }, [loadDepartments]);

    useEffect(() => {
        if (selectedDepartmentId === null) {
            setIsEditing(false);
            setFormState(initialFormState);
            setTeacherFormState(initialTeacherFormState);
            setSelectedTeacherId(null);
            setIsEditingTeacher(false);
            setTeacherSearchQuery("");
            setBatchFormState(initialBatchFormState);
            setSelectedBatchId(null);
            setIsEditingBatch(false);
            setBatchSearchQuery("");
        } else {
            const department = departments.find((d) => d.id === selectedDepartmentId);
            if (department) {
                setFormState({
                    name: department.name,
                    abbreviation: department.abbreviation,
                });
            }
        }
    }, [selectedDepartmentId, departments]);

    const resetForm = () => {
        setFormState(initialFormState);
        setIsEditing(false);
        setError(null);
        setSuccessMessage(null);
    };

    const resetFormTeacher = () => {
        setTeacherFormState(initialTeacherFormState);
        setIsEditingTeacher(false);
        setSelectedTeacherId(null);
        setError(null);
        setSuccessMessage(null);
    };

    const resetFormBatch = () => {
        setBatchFormState(initialBatchFormState);
        setIsEditingBatch(false);
        setSelectedBatchId(null);
        setError(null);
        setSuccessMessage(null);
    };

    const handleSelectTeacher = (teacher: Teacher) => {
        setSelectedTeacherId(teacher.id);
        setTeacherFormState({
            name: teacher.name,
            abbreviation: teacher.abbreviation,
            specializationIds: teacher.specializationIds,
        });
        setIsEditingTeacher(true);
        setError(null);
        setSuccessMessage(null);
    };

    const handleSelectDepartment = (department: Department) => {
        setSelectedDepartmentId(department.id);
        setFormState({
            name: department.name,
            abbreviation: department.abbreviation,
        });
        setTeacherFormState(initialTeacherFormState);
        setSelectedTeacherId(null);
        setIsEditingTeacher(false);
        setTeacherSearchQuery("");
        setBatchFormState(initialBatchFormState);
        setSelectedBatchId(null);
        setIsEditingBatch(false);
        setBatchSearchQuery("");
        setIsEditing(true);
        setSuccessMessage(null);
        setError(null);
    };

    const handleCreateMode = () => {
        setSelectedDepartmentId(null);
        setFormState(initialFormState);
        setTeacherFormState(initialTeacherFormState);
        setSelectedTeacherId(null);
        setIsEditing(false);
        setIsEditingTeacher(false);
        setTeacherSearchQuery("");
        setBatchFormState(initialBatchFormState);
        setSelectedBatchId(null);
        setIsEditingBatch(false);
        setBatchSearchQuery("");
        setSuccessMessage(null);
        setError(null);
    };

    const validateForm = () => {
        if (!formState.name.trim()) {
            setError("Department name is required.");
            return false;
        }
        if (!formState.abbreviation.trim()) {
            setError("Department abbreviation is required.");
            return false;
        }
        return true;
    };

    const validateTeacherForm = (): boolean => {
        if (!teacherFormState.name.trim()) {
            setError("Teacher name is required.");
            return false;
        }
        if (!teacherFormState.abbreviation.trim()) {
            setError("Teacher abbreviation is required.");
            return false;
        }
        if (teacherFormState.specializationIds.length === 0) {
            setError("At least one specialization subject is required.");
            return false;
        }
        const parsedDepartmentId = Number(selectedDepartment?.id);
        if (!Number.isInteger(parsedDepartmentId) || parsedDepartmentId <= 0) {
            setError("Valid department is required.");
            return false;
        }
        return true;
    };

    const validateBatchForm = (): boolean => {
        if (!batchFormState.name.trim()) {
            setError("Batch name is required.");
            return false;
        }

        const parsedSemester = Number(batchFormState.semester);
        if (!Number.isInteger(parsedSemester) || parsedSemester < 1) {
            setError("Semester must be a valid integer greater than or equal to 1.");
            return false;
        }

        const parsedDepartmentId = Number(selectedDepartment?.id);
        if (!Number.isInteger(parsedDepartmentId) || parsedDepartmentId <= 0) {
            setError("Valid department is required.");
            return false;
        }

        if (batchFormState.subjectIds.some((subjectId) => !Number.isInteger(subjectId) || subjectId <= 0)) {
            setError("Selected subjects are invalid.");
            return false;
        }

        return true;
    };

    const handleSubmit = async (event: React.FormEvent) => {
        event.preventDefault();
        setSuccessMessage(null);
        setError(null);

        if (!validateForm()) {
            return;
        }

        setIsSavingDepartment(true);
        try {
            const payload = {
                name: formState.name.trim(),
                abbreviation: formState.abbreviation.trim(),
            };

            if (isEditing && selectedDepartmentId !== null) {
                const updated = await updateDepartment(selectedDepartmentId, payload);
                setDepartments((current) => current.map((department) => (department.id === updated.id ? updated : department)));
                setSuccessMessage("Department updated successfully.");
                resetForm();
            } else {
                const created = await createDepartment(payload);
                setDepartments((current) => [...current, created]);
                setSelectedDepartmentId(created.id);
                setSuccessMessage("Department created successfully.");
                resetForm();
            }
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsSavingDepartment(false);
        }
    };

    const handleDelete = async () => {
        if (selectedDepartmentId === null) {
            return;
        }

        if (!window.confirm("Are you sure you want to delete this department?")) {
            return;
        }

        setError(null);
        setSuccessMessage(null);
        setIsDeletingDepartment(true);
        try {
            await deleteDepartment(selectedDepartmentId);
            setDepartments((current) => current.filter((department) => department.id !== selectedDepartmentId));
            setTeachers((current) => current.filter((teacher) => teacher.departmentId !== selectedDepartmentId));
            setBatches((current) => current.filter((batch) => batch.department_id !== selectedDepartmentId));
            setSuccessMessage("Department deleted successfully.");
            setSelectedDepartmentId(null);
            resetForm();
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsDeletingDepartment(false);
        }
    };

    const handleSubmitTeacher = async (event: React.FormEvent) => {
        event.preventDefault();
        setError(null);
        setSuccessMessage(null);

        if (!validateTeacherForm()) {
            return;
        }

        const payload = {
            name: teacherFormState.name.trim(),
            abbreviation: teacherFormState.abbreviation.trim(),
            specializationIds: teacherFormState.specializationIds,
            departmentId: Number(selectedDepartment?.id),
        };

        setIsSavingTeacher(true);
        try {
            if (isEditingTeacher && selectedTeacherId !== null) {
                const updated = await updateTeacher(selectedTeacherId, payload);
                setTeachers((current) => current.map((teacher) => (teacher.id === updated.id ? updated : teacher)));
                setSuccessMessage("Teacher updated successfully.");
                setTeacherFormState({
                    name: updated.name,
                    abbreviation: updated.abbreviation,
                    specializationIds: updated.specializationIds,
                });
            } else {
                const created = await createTeacher(payload);
                setTeachers((current) => [created, ...current]);
                setSelectedTeacherId(created.id);
                setIsEditingTeacher(true);
                setTeacherFormState({
                    name: created.name,
                    abbreviation: created.abbreviation,
                    specializationIds: created.specializationIds,
                });
                setSuccessMessage("Teacher created successfully.");
            }
        } catch (err) {
            setError(resolveTeacherErrorMessage(err));
        } finally {
            setIsSavingTeacher(false);
        }
    };

    const handleDeleteTeacher = async (teacherId?: number) => {
        const targetTeacherId = teacherId ?? selectedTeacherId;
        if (targetTeacherId === null) {
            return;
        }

        if (!window.confirm("Are you sure you want to delete this teacher?")) {
            return;
        }

        setError(null);
        setSuccessMessage(null);
        setDeletingTeacherId(targetTeacherId);
        try {
            await deleteTeacher(targetTeacherId);
            setTeachers((current) => current.filter((teacher) => teacher.id !== targetTeacherId));
            setSuccessMessage("Teacher deleted successfully.");
            if (selectedTeacherId === targetTeacherId) {
                resetFormTeacher();
            }
        } catch (err) {
            setError(resolveTeacherErrorMessage(err));
        } finally {
            setDeletingTeacherId(null);
        }
    };

    const handleSelectBatch = (batch: Batch) => {
        setSelectedBatchId(batch.id);
        setBatchFormState({
            name: batch.name,
            semester: String(batch.semester),
            subjectIds: batch.subject_ids,
        });
        setIsEditingBatch(true);
        setError(null);
        setSuccessMessage(null);
    };

    const handleSubmitBatch = async (event: React.FormEvent) => {
        event.preventDefault();
        setError(null);
        setSuccessMessage(null);

        if (!validateBatchForm()) {
            return;
        }

        const payload = {
            name: batchFormState.name.trim(),
            semester: Number(batchFormState.semester),
            department_id: Number(selectedDepartment?.id),
            subject_ids: batchFormState.subjectIds,
        };

        setIsSavingBatch(true);
        try {
            if (isEditingBatch && selectedBatchId !== null) {
                const updated = await updateBatch(selectedBatchId, payload);
                setBatches((current) => current.map((batch) => (batch.id === updated.id ? updated : batch)));
                setSuccessMessage("Batch updated successfully.");
                setBatchFormState({
                    name: updated.name,
                    semester: String(updated.semester),
                    subjectIds: updated.subject_ids,
                });
            } else {
                const created = await createBatch(payload);
                setBatches((current) => [created, ...current]);
                setSelectedBatchId(created.id);
                setIsEditingBatch(true);
                setBatchFormState({
                    name: created.name,
                    semester: String(created.semester),
                    subjectIds: created.subject_ids,
                });
                setSuccessMessage("Batch created successfully.");
            }
        } catch (err) {
            setError(resolveBatchErrorMessage(err));
        } finally {
            setIsSavingBatch(false);
        }
    };

    const handleDeleteBatch = async (batchId?: number) => {
        const targetBatchId = batchId ?? selectedBatchId;
        if (targetBatchId === null) {
            return;
        }

        if (!window.confirm("Are you sure you want to delete this batch?")) {
            return;
        }

        setError(null);
        setSuccessMessage(null);
        setDeletingBatchId(targetBatchId);
        try {
            await deleteBatch(targetBatchId);
            setBatches((current) => current.filter((batch) => batch.id !== targetBatchId));
            setSuccessMessage("Batch deleted successfully.");
            if (selectedBatchId === targetBatchId) {
                resetFormBatch();
            }
        } catch (err) {
            setError(resolveBatchErrorMessage(err));
        } finally {
            setDeletingBatchId(null);
        }
    };

    return (
        <div className="min-h-[calc(100vh-8rem)] bg-white/30 py-8 mt-[5rem] min-w-[calc(100%-2rem)] rounded-lg shadow-xl">
            <div className="mx-[2rem]">
                {/* Header */}
                <div className="mb-8 flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold tracking-tight text-foreground">
                            Departments
                        </h1>
                        <p className="mt-2 text-muted-foreground">
                            Manage academic departments and their faculty
                        </p>
                    </div>
                    <Button
                        onClick={handleCreateMode}
                        className="gap-2 bg-primary hover:bg-primary/90"
                    >
                        <Plus className="h-4 w-4" />
                        New Department
                    </Button>
                </div>

                {/* Alerts */}
                {error && (
                    <Alert variant="destructive" className="mb-6">
                        <AlertCircle className="h-4 w-4" />
                        <AlertDescription>{error}</AlertDescription>
                    </Alert>
                )}
                {successMessage && (
                    <Alert className="mb-6 border-green-200 bg-green-50 text-green-900">
                        <CheckCircle className="h-4 w-4" />
                        <AlertDescription>{successMessage}</AlertDescription>
                    </Alert>
                )}

                <div className="grid gap-6 xl:grid-cols-12">
                    {/* Department List */}
                    <div className="xl:col-span-3">
                        <Card className="xl:sticky xl:top-[6rem]">
                            <CardHeader className="pb-4">
                                <CardTitle className="text-lg">Departments</CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-2 overflow-y-auto">
                                {isLoading ? (
                                    <div className="py-8 text-center text-sm text-muted-foreground">
                                        Loading departments...
                                    </div>
                                ) : departments.length === 0 ? (
                                    <div className="py-8 text-center text-sm text-muted-foreground">
                                        No departments yet
                                    </div>
                                ) : (
                                    departments.map((dept) => (
                                        <button
                                            key={dept.id}
                                            onClick={() => {
                                                handleSelectDepartment(dept)
                                                setSelectedTeacherId(null);
                                            }}
                                            className={`w-full rounded-lg border-2 p-3 text-left transition-all duration-200 ${selectedDepartmentId === dept.id
                                                ? "border-primary bg-primary/10"
                                                : "border-border hover:border-primary/50 hover:bg-accent"
                                                }`}
                                        >
                                            <div className="font-semibold text-foreground">
                                                {dept.name}
                                            </div>
                                            <div className="text-xs text-muted-foreground">
                                                {dept.abbreviation}
                                            </div>
                                        </button>
                                    ))
                                )}
                            </CardContent>
                        </Card>
                    </div>

                    {/* Main Content */}
                    <div className="xl:col-span-9">
                        {selectedDepartment ? (
                            <Tabs defaultValue="Faculty" className="w-full">
                                <TabsList className="grid w-full grid-cols-4">
                                    <TabsTrigger value="Faculty">Faculty</TabsTrigger>
                                    <TabsTrigger value="Batch">Batch</TabsTrigger>
                                    <TabsTrigger value="Edit">Edit</TabsTrigger>
                                    <TabsTrigger value="Delete">Delete</TabsTrigger>
                                </TabsList>

                                <TabsContent value="Faculty" className="mt-6">
                                    <Card>
                                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
                                            <div>
                                                <CardTitle className="text-lg">Faculty Members</CardTitle>
                                                <p className="text-sm text-muted-foreground">
                                                    in {selectedDepartment.name}
                                                </p>
                                            </div>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={resetFormTeacher}
                                            >
                                                New
                                            </Button>
                                        </CardHeader>
                                        <CardContent>
                                            {isLoadingTeachers ? (
                                                <p className="text-sm text-muted-foreground">
                                                    Loading faculty members...
                                                </p>
                                            ) : (
                                                <div className="grid gap-4 xl:grid-cols-12">
                                                    <div className="space-y-3 overflow-y-auto pr-1 xl:col-span-5">
                                                        <div className="space-y-2">
                                                            <Input
                                                                value={teacherSearchQuery}
                                                                onChange={(e) => setTeacherSearchQuery(e.target.value)}
                                                                placeholder="Search by name or abbreviation"
                                                                aria-label="Search teachers"
                                                            />
                                                            <div className="flex items-center justify-between text-xs text-muted-foreground">
                                                                <span>
                                                                    Showing {filteredTeachers.length} of {teachersInSelectedDepartment.length}
                                                                </span>
                                                                {teacherSearchQuery && (
                                                                    <button
                                                                        type="button"
                                                                        className="underline underline-offset-2"
                                                                        onClick={() => setTeacherSearchQuery("")}
                                                                    >
                                                                        Clear
                                                                    </button>
                                                                )}
                                                            </div>
                                                        </div>

                                                        {teachersInSelectedDepartment.length === 0 ? (
                                                            <p className="text-sm text-muted-foreground">
                                                                No faculty members assigned
                                                            </p>
                                                        ) : filteredTeachers.length === 0 ? (
                                                            <p className="text-sm text-muted-foreground">
                                                                No matching teachers found
                                                            </p>
                                                        ) : (
                                                            filteredTeachers.map((prof) => (
                                                                <div
                                                                    key={prof.id}
                                                                    className={`flex flex-wrap gap-2 justify-between rounded-lg border p-3 transition-colors sm:grid-cols-[minmax(0,1fr)_auto] sm:items-start ${selectedTeacherId === prof.id
                                                                        ? "border-primary bg-primary/10"
                                                                        : "border-border bg-card hover:bg-accent/50"
                                                                        }`}
                                                                    onClick={() => handleSelectTeacher(prof)}
                                                                >
                                                                    <div>
                                                                        <p className="font-medium text-foreground">
                                                                            {prof.name}
                                                                        </p>
                                                                        <p className="text-xs text-muted-foreground">
                                                                            {prof.abbreviation}
                                                                        </p>
                                                                        <div className="flex flex-wrap gap-1 mt-1">
                                                                            {prof.specializationIds.map((id) => {
                                                                                const subjectName = subjectNameById.get(id);
                                                                                return subjectName ? (
                                                                                    <span
                                                                                        key={id}
                                                                                        className="rounded-full bg-primary/10 px-2 py-1 text-xs font-medium text-primary"
                                                                                    >
                                                                                        {subjectName}
                                                                                    </span>
                                                                                ) : null;
                                                                            })}
                                                                        </div>
                                                                    </div>
                                                                    <div className="flex gap-2 h-[stretch] items-center">
                                                                        <Button
                                                                            variant="destructive"
                                                                            size="sm"
                                                                            onClick={(e) => {
                                                                                e.stopPropagation();
                                                                                void handleDeleteTeacher(prof.id);
                                                                            }}
                                                                            disabled={deletingTeacherId !== null}
                                                                        >
                                                                            {deletingTeacherId === prof.id ? "Deleting..." : "Delete"}
                                                                        </Button>
                                                                    </div>
                                                                </div>
                                                            ))
                                                        )}
                                                    </div>

                                                    <Card className="self-start xl:col-span-7 xl:sticky xl:top-[6rem]">
                                                        <CardHeader>
                                                            <CardTitle className="flex items-center gap-2 text-lg justify-between">
                                                                {isEditingTeacher ? "Edit Faculty Member" : "Add New Faculty Member"}
                                                            </CardTitle>
                                                        </CardHeader>
                                                        <CardContent>
                                                            <form
                                                                className="space-y-4"
                                                                onSubmit={(e) => {
                                                                    e.preventDefault();
                                                                    handleSubmitTeacher(e);
                                                                }}
                                                            >
                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Name
                                                                    </label>
                                                                    <Input
                                                                        name="name"
                                                                        placeholder="e.g., Dr. John Doe"
                                                                        value={teacherFormState.name}
                                                                        onChange={(e) =>
                                                                            setTeacherFormState((current) => ({
                                                                                ...current,
                                                                                name: e.target.value,
                                                                            }))
                                                                        }
                                                                        required
                                                                    />
                                                                </div>
                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Abbreviation
                                                                    </label>
                                                                    <Input
                                                                        name="abbreviation"
                                                                        placeholder="e.g., JD"
                                                                        value={teacherFormState.abbreviation}
                                                                        onChange={(e) =>
                                                                            setTeacherFormState((current) => ({
                                                                                ...current,
                                                                                abbreviation: e.target.value,
                                                                            }))
                                                                        }
                                                                        required
                                                                    />
                                                                </div>
                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Specializations
                                                                    </label>
                                                                    <select
                                                                        multiple
                                                                        name="specializationIds"
                                                                        className="h-32 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                                                                        value={teacherFormState.specializationIds.map(String)}
                                                                        disabled={subjects.length === 0}
                                                                        aria-label="Select teacher specializations"
                                                                        onChange={(e) => {
                                                                            const selected = Array.from(e.target.selectedOptions).map((option) => Number(option.value));
                                                                            setTeacherFormState((current) => ({
                                                                                ...current,
                                                                                specializationIds: selected,
                                                                            }));
                                                                        }}
                                                                    >
                                                                        {subjects.map((subject) => (
                                                                            <option key={subject.id} value={subject.id}>
                                                                                {subject.name}
                                                                            </option>
                                                                        ))}
                                                                    </select>
                                                                    <p className="text-xs text-muted-foreground">
                                                                        Hold Ctrl (or Cmd on Mac) to select multiple subjects.
                                                                    </p>
                                                                    {selectedSpecializationNames.length > 0 && (
                                                                        <div className="flex flex-wrap gap-2 pt-1">
                                                                            {selectedSpecializationNames.map((name) => (
                                                                                <span
                                                                                    key={name}
                                                                                    className="rounded-full bg-primary/10 px-2 py-1 text-xs font-medium text-primary"
                                                                                >
                                                                                    {name}
                                                                                </span>
                                                                            ))}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Department
                                                                    </label>
                                                                    <Input
                                                                        name="departmentId"
                                                                        value={selectedDepartment?.name ?? "Unknown Department"}
                                                                        readOnly
                                                                    />
                                                                </div>
                                                                <div className="flex gap-2">
                                                                    <Button
                                                                        type="submit"
                                                                        disabled={isSavingTeacher}
                                                                        className="flex-1 gap-2 bg-primary hover:bg-primary/90"
                                                                    >
                                                                        {isSavingTeacher ? "Saving..." : isEditingTeacher ? "Update Faculty Member" : "Add Faculty Member"}
                                                                    </Button>
                                                                    {isEditingTeacher && (
                                                                        <Button
                                                                            type="button"
                                                                            variant="outline"
                                                                            onClick={resetFormTeacher}
                                                                        >
                                                                            Cancel
                                                                        </Button>
                                                                    )}
                                                                </div>
                                                            </form>
                                                        </CardContent>
                                                    </Card>
                                                </div>
                                            )}
                                        </CardContent>
                                    </Card>
                                </TabsContent>

                                <TabsContent value="Batch" className="mt-6">
                                    <Card>
                                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
                                            <div>
                                                <CardTitle className="text-lg">Batches</CardTitle>
                                                <p className="text-sm text-muted-foreground">
                                                    offered by {selectedDepartment.name}
                                                </p>
                                            </div>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={resetFormBatch}
                                            >
                                                New
                                            </Button>
                                        </CardHeader>
                                        <CardContent>
                                            {isLoadingBatches ? (
                                                <p className="text-sm text-muted-foreground">
                                                    Loading batches...
                                                </p>
                                            ) : (
                                                <div className="grid gap-4 xl:grid-cols-12">
                                                    <div className="space-y-3 overflow-y-auto pr-1 xl:col-span-5">
                                                        <div className="space-y-2">
                                                            <Input
                                                                value={batchSearchQuery}
                                                                onChange={(e) => setBatchSearchQuery(e.target.value)}
                                                                placeholder="Search by batch name or semester"
                                                                aria-label="Search batches"
                                                            />
                                                            <div className="flex items-center justify-between text-xs text-muted-foreground">
                                                                <span>
                                                                    Showing {filteredBatches.length} of {batchesInSelectedDepartment.length}
                                                                </span>
                                                                {batchSearchQuery && (
                                                                    <button
                                                                        type="button"
                                                                        className="underline underline-offset-2"
                                                                        onClick={() => setBatchSearchQuery("")}
                                                                    >
                                                                        Clear
                                                                    </button>
                                                                )}
                                                            </div>
                                                        </div>

                                                        {batchesInSelectedDepartment.length === 0 ? (
                                                            <p className="text-sm text-muted-foreground">
                                                                No batches assigned
                                                            </p>
                                                        ) : filteredBatches.length === 0 ? (
                                                            <p className="text-sm text-muted-foreground">
                                                                No matching batches found
                                                            </p>
                                                        ) : (
                                                            filteredBatches.map((batch) => (
                                                                <div
                                                                    key={batch.id}
                                                                    className={`flex flex-wrap gap-2 justify-between rounded-lg border p-3 transition-colors sm:grid-cols-[minmax(0,1fr)_auto] sm:items-start ${selectedBatchId === batch.id
                                                                        ? "border-primary bg-primary/10"
                                                                        : "border-border bg-card hover:bg-accent/50"
                                                                        }`}
                                                                    onClick={() => handleSelectBatch(batch)}
                                                                >
                                                                    <div>
                                                                        <p className="font-medium text-foreground">
                                                                            {batch.name}
                                                                        </p>
                                                                        <p className="text-xs text-muted-foreground">
                                                                            Semester {batch.semester}
                                                                        </p>
                                                                        <div className="mt-1 flex flex-wrap gap-1">
                                                                            {batch.subject_ids.map((id) => {
                                                                                const subjectName = subjectNameById.get(id);
                                                                                return subjectName ? (
                                                                                    <span
                                                                                        key={id}
                                                                                        className="rounded-full bg-primary/10 px-2 py-1 text-xs font-medium text-primary"
                                                                                    >
                                                                                        {subjectName}
                                                                                    </span>
                                                                                ) : null;
                                                                            })}
                                                                        </div>
                                                                    </div>
                                                                    <div className="flex gap-2 h-[stretch] items-center">
                                                                        <Button
                                                                            variant="destructive"
                                                                            size="sm"
                                                                            onClick={(e) => {
                                                                                e.stopPropagation();
                                                                                void handleDeleteBatch(batch.id);
                                                                            }}
                                                                            disabled={deletingBatchId !== null}
                                                                        >
                                                                            {deletingBatchId === batch.id ? "Deleting..." : "Delete"}
                                                                        </Button>
                                                                    </div>
                                                                </div>
                                                            ))
                                                        )}
                                                    </div>

                                                    <Card className="self-start xl:col-span-7 xl:sticky xl:top-[6rem]">
                                                        <CardHeader>
                                                            <CardTitle className="flex items-center gap-2 text-lg justify-between">
                                                                {isEditingBatch ? "Edit Batch" : "Add New Batch"}
                                                            </CardTitle>
                                                        </CardHeader>
                                                        <CardContent>
                                                            <form
                                                                className="space-y-4"
                                                                onSubmit={(e) => {
                                                                    e.preventDefault();
                                                                    handleSubmitBatch(e);
                                                                }}
                                                            >
                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Batch Name
                                                                    </label>
                                                                    <Input
                                                                        name="name"
                                                                        placeholder="e.g., FY B.Tech A"
                                                                        value={batchFormState.name}
                                                                        onChange={(e) =>
                                                                            setBatchFormState((current) => ({
                                                                                ...current,
                                                                                name: e.target.value,
                                                                            }))
                                                                        }
                                                                        required
                                                                    />
                                                                </div>

                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Semester
                                                                    </label>
                                                                    <Input
                                                                        name="semester"
                                                                        type="number"
                                                                        min={1}
                                                                        placeholder="e.g., 1"
                                                                        value={batchFormState.semester}
                                                                        onChange={(e) =>
                                                                            setBatchFormState((current) => ({
                                                                                ...current,
                                                                                semester: e.target.value,
                                                                            }))
                                                                        }
                                                                        required
                                                                    />
                                                                </div>

                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Subjects
                                                                    </label>
                                                                    <select
                                                                        multiple
                                                                        name="subject_ids"
                                                                        className="h-32 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                                                                        value={batchFormState.subjectIds.map(String)}
                                                                        disabled={subjects.length === 0}
                                                                        aria-label="Select batch subjects"
                                                                        onChange={(e) => {
                                                                            const selected = Array.from(e.target.selectedOptions).map((option) => Number(option.value));
                                                                            setBatchFormState((current) => ({
                                                                                ...current,
                                                                                subjectIds: selected,
                                                                            }));
                                                                        }}
                                                                    >
                                                                        {subjects.map((subject) => (
                                                                            <option key={subject.id} value={subject.id}>
                                                                                {subject.name}
                                                                            </option>
                                                                        ))}
                                                                    </select>
                                                                    <p className="text-xs text-muted-foreground">
                                                                        Hold Ctrl (or Cmd on Mac) to select multiple subjects.
                                                                    </p>
                                                                    {selectedBatchSubjectNames.length > 0 && (
                                                                        <div className="flex flex-wrap gap-2 pt-1">
                                                                            {selectedBatchSubjectNames.map((name) => (
                                                                                <span
                                                                                    key={name}
                                                                                    className="rounded-full bg-primary/10 px-2 py-1 text-xs font-medium text-primary"
                                                                                >
                                                                                    {name}
                                                                                </span>
                                                                            ))}
                                                                        </div>
                                                                    )}
                                                                </div>

                                                                <div className="space-y-2">
                                                                    <label className="text-sm font-medium text-foreground">
                                                                        Department
                                                                    </label>
                                                                    <Input
                                                                        name="department_id"
                                                                        value={selectedDepartment?.name ?? "Unknown Department"}
                                                                        readOnly
                                                                    />
                                                                </div>

                                                                <div className="flex gap-2">
                                                                    <Button
                                                                        type="submit"
                                                                        disabled={isSavingBatch}
                                                                        className="flex-1 gap-2 bg-primary hover:bg-primary/90"
                                                                    >
                                                                        {isSavingBatch ? "Saving..." : isEditingBatch ? "Update Batch" : "Add Batch"}
                                                                    </Button>

                                                                    {isEditingBatch && (
                                                                        <Button
                                                                            type="button"
                                                                            variant="outline"
                                                                            onClick={resetFormBatch}
                                                                        >
                                                                            Cancel
                                                                        </Button>
                                                                    )}
                                                                </div>
                                                            </form>
                                                        </CardContent>
                                                    </Card>
                                                </div>
                                            )}
                                        </CardContent>
                                    </Card>
                                </TabsContent>
                                <TabsContent value="Edit" className="mt-6">
                                    <Card>
                                        <CardHeader>
                                            <div className="flex items-center justify-between">
                                                <div>
                                                    <CardTitle>
                                                        Edit {selectedDepartment.name}
                                                    </CardTitle>
                                                    <p className="mt-2 text-sm text-muted-foreground">
                                                        Update department information
                                                    </p>
                                                </div>
                                            </div>
                                        </CardHeader>
                                        <CardContent>
                                            <form
                                                onSubmit={handleSubmit}
                                                className="space-y-6"
                                            >
                                                <div className="space-y-2">
                                                    <label className="text-sm font-medium text-foreground">
                                                        Department Name
                                                    </label>
                                                    <Input
                                                        name="name"
                                                        placeholder="e.g., Computer Science"
                                                        value={formState.name}
                                                        onChange={(e) =>
                                                            setFormState((current) => ({
                                                                ...current,
                                                                name: e.target.value,
                                                            }))
                                                        }
                                                        required
                                                    />
                                                </div>
                                                <div className="space-y-2">
                                                    <label className="text-sm font-medium text-foreground">
                                                        Abbreviation
                                                    </label>
                                                    <Input

                                                        name="abbreviation"
                                                        placeholder="e.g., CS"
                                                        value={formState.abbreviation}
                                                        onChange={(e) =>
                                                            setFormState((current) => ({
                                                                ...current,
                                                                abbreviation: e.target.value,
                                                            }))
                                                        }
                                                        required
                                                    />
                                                </div>
                                                <Button
                                                    type="submit"
                                                    disabled={isSavingDepartment}
                                                    className="flex-1 gap-2 bg-primary hover:bg-primary/90"
                                                >
                                                    {isSavingDepartment ? "Saving..." : "Update Department"}
                                                </Button>
                                            </form>
                                        </CardContent>
                                    </Card>
                                </TabsContent>
                                <TabsContent value="Delete" className="mt-6">
                                    <Card>
                                        <CardHeader>
                                            <div className="flex items-center justify-between">
                                                <div>
                                                    <CardTitle>
                                                        Delete {selectedDepartment.name} Department
                                                    </CardTitle>
                                                    <p className="mt-2 text-sm text-muted-foreground">
                                                        This action cannot be undone
                                                    </p>
                                                </div>
                                            </div>
                                        </CardHeader>
                                        <CardContent>
                                            <Button
                                                variant="destructive"
                                                className="w-full"
                                                onClick={handleDelete}
                                                disabled={isDeletingDepartment}
                                            >
                                                {isDeletingDepartment ? "Deleting..." : "Delete Department"}
                                            </Button>
                                        </CardContent>
                                    </Card>
                                </TabsContent>
                            </Tabs>
                        ) : (
                            <div className="h-full rounded-lg border-2 border-dashed border-border p-8">
                                <Card>
                                    <CardHeader>
                                        <CardTitle className="text-lg">
                                            Add new department
                                        </CardTitle>
                                        <p className="mt-2 text-sm text-muted-foreground">
                                            Get started by creating a new department
                                        </p>
                                    </CardHeader>
                                    <CardContent>
                                        <form

                                            onSubmit={handleSubmit}
                                            className="space-y-6"
                                        >
                                            <div className="space-y-2">
                                                <label className="text-sm font-medium text-foreground">
                                                    Department Name
                                                </label>
                                                <Input
                                                    name="name"
                                                    placeholder="e.g., Computer Science"
                                                    value={formState.name}
                                                    onChange={(e) =>
                                                        setFormState((current) => ({
                                                            ...current,
                                                            name: e.target.value,
                                                        }))
                                                    }
                                                    required
                                                />
                                            </div>
                                            <div className="space-y-2">
                                                <label className="text-sm font-medium text-foreground">
                                                    Abbreviation
                                                </label>
                                                <Input

                                                    name="abbreviation"
                                                    placeholder="e.g., CS"
                                                    value={formState.abbreviation}
                                                    onChange={(e) =>
                                                        setFormState((current) => ({
                                                            ...current,
                                                            abbreviation: e.target.value,
                                                        }))
                                                    }
                                                    required
                                                />
                                            </div>
                                            <Button
                                                type="submit"
                                                disabled={isSavingDepartment}
                                                className="flex-1 gap-2 bg-primary hover:bg-primary/90"
                                            >
                                                {isSavingDepartment ? "Saving..." : "Update Department"}
                                            </Button>
                                        </form>
                                    </CardContent>
                                </Card>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

export default Departments;