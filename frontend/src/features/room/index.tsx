'use client';

import { useEffect, useMemo, useState } from "react";
import { AlertCircle, CheckCircle, Plus } from "lucide-react";

import { listDepartments, type Department } from "@/features/departments/departmentApi";
import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import type { ApiError } from "@/shared/api/http";
import { createRoom, deleteRoom, listRooms, type Room, type RoomPayload, updateRoom } from "./roomApi";

type RoomFormState = {
  name: string;
  isLab: string;
  departmentId: string;
};

const initialFormState: RoomFormState = {
  name: "",
  isLab: "false",
  departmentId: "",
};

const resolveErrorMessage = (error: unknown): string => {
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

  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }

  return "Something went wrong while processing the room request.";
};

const formatDepartmentName = (department: Department | undefined): string => {
  if (!department) {
    return "Common room";
  }

  return department.abbreviation.trim().length > 0
    ? `${department.name} (${department.abbreviation})`
    : department.name;
};

function Rooms() {
  const [rooms, setRooms] = useState<Room[]>([]);
  const [departments, setDepartments] = useState<Department[]>([]);
  const [selectedRoomId, setSelectedRoomId] = useState<number | null>(null);
  const [formState, setFormState] = useState<RoomFormState>(initialFormState);

  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const selectedRoom = useMemo(
    () => rooms.find((room) => room.id === selectedRoomId) ?? null,
    [rooms, selectedRoomId]
  );

  const departmentById = useMemo(
    () => new Map<number, Department>(departments.map((department) => [department.id, department])),
    [departments]
  );

  const sortedRooms = useMemo(
    () => [...rooms].sort((left, right) => left.name.localeCompare(right.name)),
    [rooms]
  );

  const loadRoomData = async () => {
    setIsLoading(true);
    setError(null);

    try {
      const [roomResponse, departmentResponse] = await Promise.all([listRooms(), listDepartments()]);
      setRooms(roomResponse);
      setDepartments(departmentResponse);
    } catch (err) {
      setError(resolveErrorMessage(err));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadRoomData();
  }, []);

  const resetForm = () => {
    setSelectedRoomId(null);
    setFormState(initialFormState);
    setError(null);
    setSuccessMessage(null);
  };

  const handleSelectRoom = (room: Room) => {
    setSelectedRoomId(room.id);
    setFormState({
      name: room.name,
      isLab: room.isLab ? "true" : "false",
      departmentId: room.departmentId === null ? "" : String(room.departmentId),
    });
    setError(null);
    setSuccessMessage(null);
  };

  const buildPayload = (): RoomPayload | null => {
    if (!formState.name.trim()) {
      setError("Room name is required.");
      return null;
    }

    const departmentId = formState.departmentId.trim().length > 0 ? Number(formState.departmentId) : null;
    if (departmentId !== null && (!Number.isInteger(departmentId) || departmentId <= 0)) {
      setError("Department must be selected from the list or left as Common room.");
      return null;
    }

    return {
      name: formState.name.trim(),
      isLab: formState.isLab === "true",
      departmentId,
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
      if (selectedRoomId !== null) {
        const updated = await updateRoom(selectedRoomId, payload);
        setRooms((current) => current.map((room) => (room.id === updated.id ? updated : room)));
        setSuccessMessage("Room updated successfully.");
      } else {
        const created = await createRoom(payload);
        setRooms((current) => [...current, created]);
        setSuccessMessage("Room created successfully.");
      }

      resetForm();
    } catch (err) {
      setError(resolveErrorMessage(err));
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async () => {
    if (selectedRoomId === null) {
      return;
    }

    if (!window.confirm("Are you sure you want to delete this room?")) {
      return;
    }

    setError(null);
    setSuccessMessage(null);
    setIsDeleting(true);

    try {
      await deleteRoom(selectedRoomId);
      setRooms((current) => current.filter((room) => room.id !== selectedRoomId));
      setSuccessMessage("Room deleted successfully.");
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
            <h1 className="text-3xl font-bold tracking-tight text-foreground">Rooms</h1>
            <p className="mt-2 text-muted-foreground">Create and manage lecture rooms, labs, and common rooms</p>
          </div>
          <Button onClick={resetForm} className="gap-2 bg-primary hover:bg-primary/90">
            <Plus className="h-4 w-4" />
            New Room
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
                <CardTitle className="text-lg">Room List</CardTitle>
              </CardHeader>
              <CardContent className="max-h-96 space-y-2 overflow-y-auto">
                {isLoading ? (
                  <div className="py-8 text-center text-sm text-muted-foreground">Loading rooms...</div>
                ) : sortedRooms.length === 0 ? (
                  <div className="py-8 text-center text-sm text-muted-foreground">No rooms available</div>
                ) : (
                  sortedRooms.map((room) => (
                    <button
                      key={room.id}
                      onClick={() => handleSelectRoom(room)}
                      className={`w-full rounded-lg border-2 p-3 text-left transition-all duration-200 ${
                        selectedRoomId === room.id
                          ? "border-primary bg-primary/10"
                          : "border-border hover:border-primary/50 hover:bg-accent"
                      }`}
                    >
                      <div className="font-semibold text-foreground">{room.name}</div>
                      <div className="text-xs text-muted-foreground">
                        {room.isLab ? "Lab" : "Standard room"} | {formatDepartmentName(departmentById.get(room.departmentId ?? -1))}
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
                <CardTitle>{selectedRoom ? `Edit ${selectedRoom.name}` : "Create Room"}</CardTitle>
              </CardHeader>
              <CardContent>
                <form onSubmit={handleSubmit} className="space-y-6">
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Room Name</label>
                    <Input
                      name="name"
                      value={formState.name}
                      onChange={(event) => setFormState((current) => ({ ...current, name: event.target.value }))}
                      placeholder="Room 101"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Room Type</label>
                    <select
                      name="isLab"
                      value={formState.isLab}
                      onChange={(event) => setFormState((current) => ({ ...current, isLab: event.target.value }))}
                      className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/50"
                    >
                      <option value="false">Standard room</option>
                      <option value="true">Lab</option>
                    </select>
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Department</label>
                    <select
                      name="departmentId"
                      value={formState.departmentId}
                      onChange={(event) => setFormState((current) => ({ ...current, departmentId: event.target.value }))}
                      className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/50"
                    >
                      <option value="">Common room</option>
                      {departments.map((department) => (
                        <option key={department.id} value={department.id}>
                          {department.abbreviation.trim().length > 0
                            ? `${department.name} (${department.abbreviation})`
                            : department.name}
                        </option>
                      ))}
                    </select>
                    <p className="text-xs text-muted-foreground">Leave this empty for a common room.</p>
                  </div>

                  <div className="flex flex-wrap gap-3 pt-2">
                    <Button type="submit" disabled={isSaving} className="gap-2">
                      {isSaving ? "Saving..." : selectedRoom ? "Update Room" : "Create Room"}
                    </Button>
                    {selectedRoomId !== null && (
                      <Button type="button" variant="destructive" onClick={handleDelete} disabled={isDeleting}>
                        {isDeleting ? "Deleting..." : "Delete Room"}
                      </Button>
                    )}
                    <Button type="button" variant="outline" onClick={resetForm}>
                      Reset
                    </Button>
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

export default Rooms;