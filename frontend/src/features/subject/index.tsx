'use client';

import { useEffect, useMemo, useState } from "react";
import { AlertCircle, CheckCircle, Plus } from "lucide-react";

import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import { listDepartments, type Department } from "@/features/departments/departmentApi";
import {
  createSubject,
  deleteSubject,
  listSubjects,
  updateSubject,
  type Subject,
  type SubjectPayload,
} from "@/features/subject/subjectApi";

type SubjectFormState = {
  name: string;
  departmentIds: string[];
  lecture: string;
  tutorial: string;
  practical: string;
};

const initialFormState: SubjectFormState = {
  name: "",
  departmentIds: [],
  lecture: "",
  tutorial: "",
  practical: "",
};

const resolveErrorMessage = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return "Something went wrong while processing the subject request.";
};

function Subjects() {
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [departments, setDepartments] = useState<Department[]>([]);
  const [selectedSubjectId, setSelectedSubjectId] = useState<number | null>(null);
  const [formState, setFormState] = useState<SubjectFormState>(initialFormState);

  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const selectedSubject = useMemo(
    () => subjects.find((subject) => subject.id === selectedSubjectId) ?? null,
    [subjects, selectedSubjectId]
  );

  const loadSubjects = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const [subjectResponse, departmentResponse] = await Promise.all([
        listSubjects(),
        listDepartments(),
      ]);
      setSubjects(subjectResponse);
      setDepartments(departmentResponse);
    } catch (err) {
      setError(resolveErrorMessage(err));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadSubjects();
  }, []);

  const resetForm = () => {
    setSelectedSubjectId(null);
    setFormState(initialFormState);
    setError(null);
    setSuccessMessage(null);
  };

  const handleSelectSubject = (subject: Subject) => {
    setSelectedSubjectId(subject.id);
    setFormState({
      name: subject.name,
      departmentIds: subject.department_ids.map(String),
      lecture: String(subject.lecture),
      tutorial: String(subject.tutorial),
      practical: String(subject.practical),
    });
    setError(null);
    setSuccessMessage(null);
  };

  const parseHours = (value: string): number | null => {
    if (!value.trim()) {
      return null;
    }
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 0) {
      return null;
    }
    return parsed;
  };

  const buildPayload = (): SubjectPayload | null => {
    const departmentIds = formState.departmentIds
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0);
    const lecture = parseHours(formState.lecture);
    const tutorial = parseHours(formState.tutorial);
    const practical = parseHours(formState.practical);

    if (!formState.name.trim()) {
      setError("Subject name is required.");
      return null;
    }
    if (departmentIds.length === 0) {
      setError("Please select at least one department.");
      return null;
    }
    if (lecture === null) {
      setError("Lecture must be a valid number greater than or equal to 0.");
      return null;
    }
    if (tutorial === null) {
      setError("Tutorial must be a valid number greater than or equal to 0.");
      return null;
    }
    if (practical === null) {
      setError("Practical must be a valid number greater than or equal to 0.");
      return null;
    }

    return {
      name: formState.name.trim(),
      department_ids: departmentIds,
      lecture,
      tutorial,
      practical,
    };
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    setSuccessMessage(null);

    const payload = buildPayload();
    if (!payload) {
      return;
    }

    setIsSaving(true);
    try {
      if (selectedSubjectId !== null) {
        const updated = await updateSubject(selectedSubjectId, payload);
        setSubjects((current) => current.map((subject) => (subject.id === updated.id ? updated : subject)));
        setSuccessMessage("Subject updated successfully.");
      } else {
        const created = await createSubject(payload);
        setSubjects((current) => [...current, created]);
        setSuccessMessage("Subject created successfully.");
      }
      resetForm();
    } catch (err) {
      setError(resolveErrorMessage(err));
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async () => {
    if (selectedSubjectId === null) {
      return;
    }

    if (!window.confirm("Are you sure you want to delete this subject?")) {
      return;
    }

    setError(null);
    setSuccessMessage(null);
    setIsDeleting(true);
    try {
      await deleteSubject(selectedSubjectId);
      setSubjects((current) => current.filter((subject) => subject.id !== selectedSubjectId));
      setSuccessMessage("Subject deleted successfully.");
      resetForm();
    } catch (err) {
      setError(resolveErrorMessage(err));
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <div className="min-h-[calc(100vh-8rem)] bg-white/30 p-8 mt-[5rem] min-w-[calc(100%-2rem)] rounded-lg shadow-xl">
      <div className="mx-auto max-w-7xl">
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-foreground">Subjects</h1>
            <p className="mt-2 text-muted-foreground">Create and manage subjects with lecture/tutorial/practical hours</p>
          </div>
          <Button onClick={resetForm} className="gap-2 bg-primary hover:bg-primary/90">
            <Plus className="h-4 w-4" />
            New Subject
          </Button>
        </div>

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

        <div className="grid gap-8 lg:grid-cols-3">
          <div className="lg:col-span-1">
            <Card className="sticky top-8">
              <CardHeader className="pb-4">
                <CardTitle className="text-lg">Subject List</CardTitle>
              </CardHeader>
              <CardContent className="max-h-96 space-y-2 overflow-y-auto">
                {isLoading ? (
                  <div className="py-8 text-center text-sm text-muted-foreground">Loading subjects...</div>
                ) : subjects.length === 0 ? (
                  <div className="py-8 text-center text-sm text-muted-foreground">No subjects available</div>
                ) : (
                  subjects.map((subject) => (
                    <button
                      key={subject.id}
                      onClick={() => handleSelectSubject(subject)}
                      className={`w-full rounded-lg border-2 p-3 text-left transition-all duration-200 ${
                        selectedSubjectId === subject.id
                          ? "border-primary bg-primary/10"
                          : "border-border hover:border-primary/50 hover:bg-accent"
                      }`}
                    >
                      <div className="font-semibold text-foreground">{subject.name}</div>
                      <div className="text-xs text-muted-foreground">
                        Dept: {subject.department_ids
                          .map((id) => departments.find((department) => department.id === id)?.abbreviation)
                          .filter(Boolean)
                          .join(", ") || "-"} | {" "}
                        L: {subject.lecture} | T: {subject.tutorial} | P: {subject.practical}
                      </div>
                    </button>
                  ))
                )}
              </CardContent>
            </Card>
          </div>

          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <CardTitle>{selectedSubject ? `Edit ${selectedSubject.name}` : "Create Subject"}</CardTitle>
              </CardHeader>
              <CardContent>
                <form onSubmit={handleSubmit} className="space-y-6">
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Subject Name</label>
                    <Input
                      name="name"
                      placeholder="e.g., Data Structures"
                      value={formState.name}
                      onChange={(e) => setFormState((current) => ({ ...current, name: e.target.value }))}
                      required
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Department</label>
                    <select
                      name="departmentIds"
                      multiple
                      value={formState.departmentIds}
                      onChange={(e) => {
                        const selected = Array.from(e.target.selectedOptions).map((option) => option.value);
                        setFormState((current) => ({ ...current, departmentIds: selected }));
                      }}
                      className="min-h-28 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                      required
                    >
                      {departments.map((department) => (
                        <option key={department.id} value={department.id}>
                          {department.name} ({department.abbreviation})
                        </option>
                      ))}
                    </select>
                    <p className="text-xs text-muted-foreground">Hold Ctrl/Cmd to select multiple departments.</p>
                  </div>

                  <div className="grid gap-4 sm:grid-cols-3">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-foreground">Lecture</label>
                      <Input
                        name="lecture"
                        type="number"
                        min={0}
                        value={formState.lecture}
                        onChange={(e) => setFormState((current) => ({ ...current, lecture: e.target.value }))}
                        required
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-foreground">Tutorial</label>
                      <Input
                        name="tutorial"
                        type="number"
                        min={0}
                        value={formState.tutorial}
                        onChange={(e) => setFormState((current) => ({ ...current, tutorial: e.target.value }))}
                        required
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-foreground">Practical</label>
                      <Input
                        name="practical"
                        type="number"
                        min={0}
                        value={formState.practical}
                        onChange={(e) => setFormState((current) => ({ ...current, practical: e.target.value }))}
                        required
                      />
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <Button type="submit" disabled={isSaving} className="flex-1 gap-2 bg-primary hover:bg-primary/90">
                      {isSaving ? "Saving..." : selectedSubject ? "Update Subject" : "Create Subject"}
                    </Button>

                    {selectedSubject && (
                      <Button type="button" variant="destructive" onClick={handleDelete} disabled={isDeleting}>
                        {isDeleting ? "Deleting..." : "Delete"}
                      </Button>
                    )}
                  </div>
                </form>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Subjects;
