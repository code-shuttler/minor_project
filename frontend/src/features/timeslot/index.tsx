'use client';

import { useEffect, useMemo, useState } from "react";
import { AlertCircle, CheckCircle, Plus } from "lucide-react";

import type { ApiError } from "@/shared/api/http";
import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import {
    createTimeSlot,
    deleteTimeSlot,
    listTimeSlots,
    updateTimeSlot,
    type TimeSlot,
    type TimeSlotPayload,
} from "./timeslotApi";

type TimeSlotFormState = {
    day: string;
    startTime: string;
    endTime: string;
};

const weekdayOptions = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
];

const weekdayOrder = new Map(weekdayOptions.map((day, index) => [day, index]));

const initialFormState: TimeSlotFormState = {
    day: "",
    startTime: "",
    endTime: "",
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

    return "Something went wrong while processing the time slot request.";
};

const normalizeTimeForInput = (value: string): string => {
    const [hours, minutes] = value.split(":");
    if (!hours || !minutes) {
        return "";
    }
    return `${hours.padStart(2, "0")}:${minutes.padStart(2, "0")}`;
};

const toApiTime = (value: string): string => {
    const normalized = normalizeTimeForInput(value);
    return normalized ? `${normalized}:00` : value;
};

const formatDay = (value: string): string => {
    if (!value) {
        return value;
    }
    return `${value.charAt(0)}${value.slice(1).toLowerCase()}`;
};

const sortTimeSlots = (slots: TimeSlot[]): TimeSlot[] => {
    return [...slots].sort((left, right) => {
        const leftDay = weekdayOrder.get(left.day) ?? Number.MAX_SAFE_INTEGER;
        const rightDay = weekdayOrder.get(right.day) ?? Number.MAX_SAFE_INTEGER;

        if (leftDay !== rightDay) {
            return leftDay - rightDay;
        }

        return left.start_time.localeCompare(right.start_time);
    });
};

function TimeSlotPage() {
    const [timeSlots, setTimeSlots] = useState<TimeSlot[]>([]);
    const [selectedTimeSlotId, setSelectedTimeSlotId] = useState<number | null>(null);
    const [formState, setFormState] = useState<TimeSlotFormState>(initialFormState);

    const [isLoading, setIsLoading] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const [isDeleting, setIsDeleting] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [successMessage, setSuccessMessage] = useState<string | null>(null);

    const selectedTimeSlot = useMemo(
        () => timeSlots.find((slot) => slot.id === selectedTimeSlotId) ?? null,
        [timeSlots, selectedTimeSlotId]
    );

    const sortedTimeSlots = useMemo(() => sortTimeSlots(timeSlots), [timeSlots]);

    const loadTimeSlots = async () => {
        setIsLoading(true);
        setError(null);
        try {
            const response = await listTimeSlots();
            setTimeSlots(response);
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void loadTimeSlots();
    }, []);

    const resetForm = (preserveMessage = false) => {
        setSelectedTimeSlotId(null);
        setFormState(initialFormState);
        setError(null);
        if (!preserveMessage) {
            setSuccessMessage(null);
        }
    };

    const handleSelectTimeSlot = (slot: TimeSlot) => {
        setSelectedTimeSlotId(slot.id);
        setFormState({
            day: slot.day,
            startTime: normalizeTimeForInput(slot.start_time),
            endTime: normalizeTimeForInput(slot.end_time),
        });
        setError(null);
        setSuccessMessage(null);
    };

    const buildPayload = (): TimeSlotPayload | null => {
        const day = formState.day.trim().toUpperCase();
        const startTime = normalizeTimeForInput(formState.startTime);
        const endTime = normalizeTimeForInput(formState.endTime);

        if (!weekdayOptions.includes(day)) {
            setError("Please select a valid weekday.");
            return null;
        }

        if (!startTime) {
            setError("Start time is required.");
            return null;
        }

        if (!endTime) {
            setError("End time is required.");
            return null;
        }

        if (startTime >= endTime) {
            setError("Start time must be earlier than end time.");
            return null;
        }

        return {
            day,
            start_time: toApiTime(startTime),
            end_time: toApiTime(endTime),
        };
    };

    const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        setError(null);
        setSuccessMessage(null);

        const payload = buildPayload();
        if (!payload) {
            return;
        }

        setIsSaving(true);
        try {
            if (selectedTimeSlotId !== null) {
                const updated = await updateTimeSlot(selectedTimeSlotId, payload);
                setTimeSlots((current) => current.map((slot) => (slot.id === updated.id ? updated : slot)));
                resetForm(true);
                setSuccessMessage("Time slot updated successfully.");
            } else {
                const created = await createTimeSlot(payload);
                setTimeSlots((current) => [...current, created]);
                resetForm(true);
                setSuccessMessage("Time slot created successfully.");
            }
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsSaving(false);
        }
    };

    const handleDelete = async () => {
        if (selectedTimeSlotId === null) {
            return;
        }

        if (!window.confirm("Are you sure you want to delete this time slot?")) {
            return;
        }

        setError(null);
        setSuccessMessage(null);
        setIsDeleting(true);
        try {
            await deleteTimeSlot(selectedTimeSlotId);
            setTimeSlots((current) => current.filter((slot) => slot.id !== selectedTimeSlotId));
            resetForm(true);
            setSuccessMessage("Time slot deleted successfully.");
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
                        <h1 className="text-3xl font-bold tracking-tight text-foreground">Time Slots</h1>
                        <p className="mt-2 text-muted-foreground">Create and manage weekly time periods for timetable scheduling</p>
                    </div>
                    <Button onClick={() => resetForm()} className="gap-2 bg-primary hover:bg-primary/90" type="button">
                        <Plus className="h-4 w-4" />
                        New Time Slot
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
                                <CardTitle className="text-lg">Time Slot List</CardTitle>
                            </CardHeader>
                            <CardContent className="max-h-96 space-y-2 overflow-y-auto">
                                {isLoading ? (
                                    <div className="py-8 text-center text-sm text-muted-foreground">Loading time slots...</div>
                                ) : sortedTimeSlots.length === 0 ? (
                                    <div className="py-8 text-center text-sm text-muted-foreground">No time slots available</div>
                                ) : (
                                    sortedTimeSlots.map((slot) => (
                                        <button
                                            key={slot.id}
                                            onClick={() => handleSelectTimeSlot(slot)}
                                            className={`w-full rounded-lg border-2 p-3 text-left transition-all duration-200 ${
                                                selectedTimeSlotId === slot.id
                                                    ? "border-primary bg-primary/10"
                                                    : "border-border hover:border-primary/50 hover:bg-accent"
                                            }`}
                                        >
                                            <div className="font-semibold text-foreground">{formatDay(slot.day)}</div>
                                            <div className="text-xs text-muted-foreground">
                                                {normalizeTimeForInput(slot.start_time)} - {normalizeTimeForInput(slot.end_time)}
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
                                <CardTitle>
                                    {selectedTimeSlot
                                        ? `Edit ${formatDay(selectedTimeSlot.day)} ${normalizeTimeForInput(selectedTimeSlot.start_time)}-${normalizeTimeForInput(selectedTimeSlot.end_time)}`
                                        : "Create Time Slot"}
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <form onSubmit={handleSubmit} className="space-y-6">
                                    <div className="space-y-2">
                                        <label className="text-sm font-medium text-foreground">Day</label>
                                        <select
                                            name="day"
                                            value={formState.day}
                                            onChange={(event) => setFormState((current) => ({ ...current, day: event.target.value }))}
                                            className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                                            required
                                        >
                                            <option value="">Select day</option>
                                            {weekdayOptions.map((day) => (
                                                <option key={day} value={day}>
                                                    {formatDay(day)}
                                                </option>
                                            ))}
                                        </select>
                                    </div>

                                    <div className="grid gap-4 sm:grid-cols-2">
                                        <div className="space-y-2">
                                            <label className="text-sm font-medium text-foreground">Start Time</label>
                                            <Input
                                                name="startTime"
                                                type="time"
                                                value={formState.startTime}
                                                onChange={(event) =>
                                                    setFormState((current) => ({
                                                        ...current,
                                                        startTime: event.target.value,
                                                    }))
                                                }
                                                required
                                            />
                                        </div>

                                        <div className="space-y-2">
                                            <label className="text-sm font-medium text-foreground">End Time</label>
                                            <Input
                                                name="endTime"
                                                type="time"
                                                value={formState.endTime}
                                                onChange={(event) =>
                                                    setFormState((current) => ({
                                                        ...current,
                                                        endTime: event.target.value,
                                                    }))
                                                }
                                                required
                                            />
                                        </div>
                                    </div>

                                    <div className="flex gap-3">
                                        <Button type="submit" disabled={isSaving} className="flex-1 gap-2 bg-primary hover:bg-primary/90">
                                            {isSaving ? "Saving..." : selectedTimeSlot ? "Update Time Slot" : "Create Time Slot"}
                                        </Button>

                                        {selectedTimeSlot && (
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

export default TimeSlotPage;