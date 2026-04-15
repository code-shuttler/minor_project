'use client';

import { useEffect, useMemo, useState } from "react";
import { AlertCircle, CheckCircle, Filter, WandSparkles } from "lucide-react";

import { listBatches, type Batch } from "@/features/batches/batchApi";
import { listRooms, type Room } from "@/features/room/roomApi";
import { listSubjects, type Subject } from "@/features/subject/subjectApi";
import { listTeachers, type Teacher } from "@/features/teacher/teacherApi";
import type { ApiError } from "@/shared/api/http";
import { Alert, AlertDescription } from "@/shared/components/ui/alert";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import {
    autoGenerateTimetables,
    listTimeSlots,
    type TimeSlot,
    listTimetables,
    type Timetable,
} from "./timetableApi";

const weekdayOrder = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
];

const weekdaySortOrder = new Map(weekdayOrder.map((day, index) => [day, index]));

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

    return "Something went wrong while processing the timetable request.";
};

const resolveAutoGenerationError = (error: unknown): string => {
    const apiError = error as ApiError;
    const baseMessage = resolveErrorMessage(error);

    const isTimeoutStatus = apiError?.status === 408;
    const looksLikeTimeout = /timed out|time limit|interrupted/i.test(baseMessage);

    if (isTimeoutStatus || looksLikeTimeout) {
        return "Auto-generation took too long to finish. Try selecting fewer batches, or ask an admin to increase TIMETABLE_MAX_GENERATION_MS on the backend.";
    }

    return baseMessage;
};

const formatDay = (value: string): string => {
    if (!value) {
        return value;
    }
    return `${value.charAt(0)}${value.slice(1).toLowerCase()}`;
};

const formatTime = (value: string): string => {
    if (!value) {
        return value;
    }
    const [hours, minutes] = value.split(":");
    if (!hours || !minutes) {
        return value;
    }
    return `${hours}:${minutes}`;
};

const formatTimeRangeLabel = (slotLabel: string): string => {
    const [start, end] = slotLabel.split("-");
    return `${formatTime(start)} - ${formatTime(end)}`;
};

const sortTimetables = (
    entries: Timetable[],
    batchById: Map<number, Batch>,
    timeSlotById: Map<number, TimeSlot>
): Timetable[] => {
    return [...entries].sort((left, right) => {
        const leftTimeSlot = timeSlotById.get(left.time_slot_id);
        const rightTimeSlot = timeSlotById.get(right.time_slot_id);

        const leftDay = weekdaySortOrder.get(leftTimeSlot?.day ?? "") ?? Number.MAX_SAFE_INTEGER;
        const rightDay = weekdaySortOrder.get(rightTimeSlot?.day ?? "") ?? Number.MAX_SAFE_INTEGER;
        if (leftDay !== rightDay) {
            return leftDay - rightDay;
        }

        const leftStart = leftTimeSlot?.start_time ?? "";
        const rightStart = rightTimeSlot?.start_time ?? "";
        if (leftStart !== rightStart) {
            return leftStart.localeCompare(rightStart);
        }

        const leftBatch = batchById.get(left.batch_id)?.name ?? "";
        const rightBatch = batchById.get(right.batch_id)?.name ?? "";
        if (leftBatch !== rightBatch) {
            return leftBatch.localeCompare(rightBatch);
        }

        return left.id - right.id;
    });
};

function TimeTable() {
    const [timetables, setTimetables] = useState<Timetable[]>([]);
    const [batches, setBatches] = useState<Batch[]>([]);
    const [subjects, setSubjects] = useState<Subject[]>([]);
    const [teachers, setTeachers] = useState<Teacher[]>([]);
    const [timeSlots, setTimeSlots] = useState<TimeSlot[]>([]);
    const [rooms, setRooms] = useState<Room[]>([]);

    const [selectedTimetableId, setSelectedTimetableId] = useState<number | null>(null);
    const [selectedBatchIds, setSelectedBatchIds] = useState<Set<number>>(new Set());
    const [batchFilter, setBatchFilter] = useState<string>("all");
    const [dayFilter, setDayFilter] = useState<string>("all");
    const [subjectFilter, setSubjectFilter] = useState<string>("all");
    const [roomTypeFilter, setRoomTypeFilter] = useState<"all" | "lab" | "classroom">("all");
    const [searchFilter, setSearchFilter] = useState<string>("");

    const [isLoading, setIsLoading] = useState(false);
    const [isGenerating, setIsGenerating] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [successMessage, setSuccessMessage] = useState<string | null>(null);

    const batchById = useMemo(
        () => new Map<number, Batch>(batches.map((batch) => [batch.id, batch])),
        [batches]
    );

    const subjectById = useMemo(
        () => new Map<number, Subject>(subjects.map((subject) => [subject.id, subject])),
        [subjects]
    );

    const teacherById = useMemo(
        () => new Map<number, Teacher>(teachers.map((teacher) => [teacher.id, teacher])),
        [teachers]
    );

    const timeSlotById = useMemo(
        () => new Map<number, TimeSlot>(timeSlots.map((slot) => [slot.id, slot])),
        [timeSlots]
    );

    const roomById = useMemo(
        () => new Map<number, Room>(rooms.map((room) => [room.id, room])),
        [rooms]
    );

    const sortedBatches = useMemo(
        () => [...batches].sort((left, right) => left.name.localeCompare(right.name)),
        [batches]
    );

    const sortedSubjects = useMemo(
        () => [...subjects].sort((left, right) => left.name.localeCompare(right.name)),
        [subjects]
    );

    const normalizedSearchFilter = useMemo(() => searchFilter.trim().toLowerCase(), [searchFilter]);

    const sortedTimetables = useMemo(
        () => sortTimetables(timetables, batchById, timeSlotById),
        [timetables, batchById, timeSlotById]
    );

    const filteredTimetables = useMemo(() => {
        return sortedTimetables.filter((entry) => {
            if (batchFilter !== "all" && entry.batch_id !== Number(batchFilter)) {
                return false;
            }

            if (subjectFilter !== "all" && entry.subject_id !== Number(subjectFilter)) {
                return false;
            }

            if (dayFilter !== "all") {
                const slotDay = timeSlotById.get(entry.time_slot_id)?.day ?? "";
                if (slotDay !== dayFilter) {
                    return false;
                }
            }

            if (roomTypeFilter !== "all") {
                const room = roomById.get(entry.room_id);
                const isLab = room?.isLab === true;
                if (roomTypeFilter === "lab" && !isLab) {
                    return false;
                }
                if (roomTypeFilter === "classroom" && isLab) {
                    return false;
                }
            }

            if (normalizedSearchFilter) {
                const subjectName = subjectById.get(entry.subject_id)?.name ?? "";
                const teacher = teacherById.get(entry.teacher_id);
                const teacherName = teacher?.name ?? "";
                const teacherAbbreviation = teacher?.abbreviation ?? "";
                const roomName = roomById.get(entry.room_id)?.name ?? "";
                const batchName = batchById.get(entry.batch_id)?.name ?? "";

                const haystack = `${subjectName} ${teacherName} ${teacherAbbreviation} ${roomName} ${batchName}`
                    .trim()
                    .toLowerCase();

                if (!haystack.includes(normalizedSearchFilter)) {
                    return false;
                }
            }

            return true;
        });
    }, [
        sortedTimetables,
        batchFilter,
        dayFilter,
        subjectFilter,
        roomTypeFilter,
        normalizedSearchFilter,
        subjectById,
        teacherById,
        roomById,
        batchById,
        timeSlotById,
    ]);

    const hasActiveFilters =
        batchFilter !== "all" ||
        dayFilter !== "all" ||
        subjectFilter !== "all" ||
        roomTypeFilter !== "all" ||
        normalizedSearchFilter.length > 0;

    const dayFilterOptions = useMemo(() => {
        const days = new Set<string>();
        timeSlots.forEach((slot) => days.add(slot.day));
        return weekdayOrder.filter((day) => days.has(day));
    }, [timeSlots]);

    const timeFilterOptions = useMemo(() => {
        const times = new Set<string>();
        timeSlots.forEach((slot) => times.add(`${slot.start_time}-${slot.end_time}`));
        return Array.from(times).sort((left, right) => left.localeCompare(right));
    }, [timeSlots]);

    const visibleTimeColumns = useMemo(() => {
        if (filteredTimetables.length === 0) {
            return timeFilterOptions;
        }

        const visibleTimes = new Set<string>();
        filteredTimetables.forEach((entry) => {
            const slot = timeSlotById.get(entry.time_slot_id);
            if (!slot) {
                return;
            }
            visibleTimes.add(`${slot.start_time}-${slot.end_time}`);
        });

        return timeFilterOptions.filter((time) => visibleTimes.has(time));
    }, [filteredTimetables, timeFilterOptions, timeSlotById]);

    const visibleDayRows = useMemo(() => {
        const daysWithEntries = new Set<string>();
        filteredTimetables.forEach((entry) => {
            const day = timeSlotById.get(entry.time_slot_id)?.day;
            if (day) {
                daysWithEntries.add(day);
            }
        });

        if (dayFilter === "all") {
            if (daysWithEntries.size === 0) {
                return dayFilterOptions;
            }
            return dayFilterOptions.filter((day) => daysWithEntries.has(day));
        }
        return dayFilterOptions.includes(dayFilter) ? [dayFilter] : [];
    }, [dayFilter, dayFilterOptions, filteredTimetables, timeSlotById]);

    const selectedBatchNames = useMemo(
        () => sortedBatches.filter((batch) => selectedBatchIds.has(batch.id)).map((batch) => batch.name),
        [sortedBatches, selectedBatchIds]
    );

    const selectedEntry = useMemo(
        () => sortedTimetables.find((entry) => entry.id === selectedTimetableId) ?? null,
        [sortedTimetables, selectedTimetableId]
    );

    const selectedEntryTimeSlot = useMemo(
        () => (selectedEntry ? timeSlotById.get(selectedEntry.time_slot_id) ?? null : null),
        [selectedEntry, timeSlotById]
    );

    const stats = useMemo(() => {
        const batchCount = new Set(filteredTimetables.map((entry) => entry.batch_id)).size;
        const teacherCount = new Set(filteredTimetables.map((entry) => entry.teacher_id)).size;
        return {
            totalEntries: filteredTimetables.length,
            batchCount,
            teacherCount,
        };
    }, [filteredTimetables]);

    const timetableCellMap = useMemo(() => {
        const cells = new Map<string, Timetable[]>();

        filteredTimetables.forEach((entry) => {
            const timeSlot = timeSlotById.get(entry.time_slot_id);
            if (!timeSlot) {
                return;
            }

            const key = `${timeSlot.day}::${timeSlot.start_time}-${timeSlot.end_time}`;
            const existing = cells.get(key) ?? [];
            existing.push(entry);
            cells.set(key, existing);
        });

        return cells;
    }, [filteredTimetables, timeSlotById]);

    const loadTimetableData = async () => {
        setIsLoading(true);
        setError(null);
        try {
            const [timetableResponse, batchResponse, subjectResponse, teacherResponse, timeSlotResponse, roomResponse] = await Promise.all([
                listTimetables(),
                listBatches(),
                listSubjects(),
                listTeachers(),
                listTimeSlots(),
                listRooms(),
            ]);

            setTimetables(timetableResponse);
            setBatches(batchResponse);
            setSubjects(subjectResponse);
            setTeachers(teacherResponse);
            setTimeSlots(timeSlotResponse);
            setRooms(roomResponse);
        } catch (err) {
            setError(resolveErrorMessage(err));
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void loadTimetableData();
    }, []);

    const resetSelection = () => {
        setSelectedTimetableId(null);
        setError(null);
        setSuccessMessage(null);
    };

    const handleSelectTimetable = (entry: Timetable) => {
        setSelectedTimetableId(entry.id);
        setError(null);
        setSuccessMessage(null);
    };

    const handleAutoGenerate = async () => {
        setError(null);
        setSuccessMessage(null);

        if (selectedBatchIds.size === 0) {
            setError("Please select at least one batch.");
            return;
        }

        setIsGenerating(true);
        try {
            const generated = await autoGenerateTimetables({ batches_id: Array.from(selectedBatchIds) });
            await loadTimetableData();
            setSelectedTimetableId(null);
            setSelectedBatchIds(new Set());
            setSuccessMessage(
                `Auto-generated ${generated.length} timetable entr${generated.length === 1 ? "y" : "ies"} for ${selectedBatchIds.size} batch(es).`
            );
        } catch (err) {
            setError(resolveAutoGenerationError(err));
        } finally {
            setIsGenerating(false);
        }
    };

    const handleBatchSelect = (batchId: number, isChecked: boolean) => {
        setSelectedBatchIds((prev) => {
            const next = new Set(prev);
            if (isChecked) {
                next.add(batchId);
            } else {
                next.delete(batchId);
            }
            return next;
        });
    };

    const handleSelectAllBatches = () => {
        setSelectedBatchIds(new Set(sortedBatches.map((batch) => batch.id)));
    };

    const handleClearBatchSelection = () => {
        setSelectedBatchIds(new Set());
    };

    const handleResetFilters = () => {
        setBatchFilter("all");
        setDayFilter("all");
        setSubjectFilter("all");
        setRoomTypeFilter("all");
        setSearchFilter("");
    };

    return (
        <div className="min-h-[calc(100vh-8rem)] bg-white/30 p-8 mt-[5rem] min-w-[calc(100%-2rem)] rounded-lg shadow-xl">
            <div className="mx-auto max-w-7xl">
                <div className="mb-8 flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold tracking-tight text-foreground">Timetable</h1>
                        <p className="mt-2 text-muted-foreground">View, filter, and manage auto-generated batch schedules</p>
                    </div>
                    <div className="flex flex-wrap items-end gap-3">
                        <Button
                            type="button"
                            variant="secondary"
                            className="gap-2"
                            onClick={handleAutoGenerate}
                            disabled={isGenerating || isLoading || selectedBatchIds.size === 0}
                        >
                            <WandSparkles className="h-4 w-4" />
                            {isGenerating ? "Generating..." : `Auto Generate (${selectedBatchIds.size})`}
                        </Button>
                        <Button onClick={resetSelection} className="gap-2 bg-primary hover:bg-primary/90" type="button">
                            Clear Selection
                        </Button>
                    </div>
                </div>

                <div className="mb-6 grid gap-3 sm:grid-cols-3">
                    <Card>
                        <CardContent className="pt-4">
                            <p className="text-xs text-muted-foreground">Entries</p>
                            <p className="text-2xl font-semibold">{stats.totalEntries}</p>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="pt-4">
                            <p className="text-xs text-muted-foreground">Batches Covered</p>
                            <p className="text-2xl font-semibold">{stats.batchCount}</p>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="pt-4">
                            <p className="text-xs text-muted-foreground">Teachers Used</p>
                            <p className="text-2xl font-semibold">{stats.teacherCount}</p>
                        </CardContent>
                    </Card>
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

                {selectedEntry && (
                    <Card className="mb-6 border-primary/30 bg-primary/5">
                        <CardContent className="pt-4">
                            <p className="text-xs text-muted-foreground">Selected Entry</p>
                            <p className="text-base font-semibold text-foreground">
                                {subjectById.get(selectedEntry.subject_id)?.name ?? `Subject #${selectedEntry.subject_id}`}
                            </p>
                            <p className="text-sm text-muted-foreground">
                                {(batchById.get(selectedEntry.batch_id)?.name ?? `Batch #${selectedEntry.batch_id}`)}
                                {" | "}
                                {(teacherById.get(selectedEntry.teacher_id)?.abbreviation ?? "")}
                                {teacherById.get(selectedEntry.teacher_id) ? " - " : ""}
                                {(teacherById.get(selectedEntry.teacher_id)?.name ?? `Teacher #${selectedEntry.teacher_id}`)}
                                {" | "}
                                {(roomById.get(selectedEntry.room_id)?.name ?? `Room #${selectedEntry.room_id}`)}
                                {" | "}
                                {(selectedEntryTimeSlot ? `${formatDay(selectedEntryTimeSlot.day)} ${formatTime(selectedEntryTimeSlot.start_time)}-${formatTime(selectedEntryTimeSlot.end_time)}` : "Time slot unavailable")}
                            </p>
                        </CardContent>
                    </Card>
                )}

                <div className="mb-6 rounded-lg border border-purple-200 bg-purple-50 p-4">
                    <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                        <h2 className="font-semibold text-foreground">Select Batches for Auto-Generation</h2>
                        <div className="flex flex-wrap gap-2">
                            <Button
                                type="button"
                                variant="outline"
                                className="h-8"
                                onClick={handleSelectAllBatches}
                                disabled={isGenerating || sortedBatches.length === 0}
                            >
                                Select All
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                className="h-8"
                                onClick={handleClearBatchSelection}
                                disabled={isGenerating || selectedBatchIds.size === 0}
                            >
                                Clear Batches
                            </Button>
                        </div>
                    </div>
                    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                        {sortedBatches.map((batch) => (
                            <label key={batch.id} className="flex cursor-pointer items-center gap-2 rounded p-2 hover:bg-purple-100">
                                <input
                                    type="checkbox"
                                    checked={selectedBatchIds.has(batch.id)}
                                    onChange={(e) => handleBatchSelect(batch.id, e.target.checked)}
                                    disabled={isGenerating}
                                    className="h-4 w-4"
                                />
                                <span className="text-sm">{batch.name} Sem {batch.semester}</span>
                            </label>
                        ))}
                    </div>
                    <p className="mt-3 text-xs text-muted-foreground">
                        {selectedBatchIds.size === 0
                            ? "No batch selected"
                            : `${selectedBatchIds.size} selected: ${selectedBatchNames.slice(0, 4).join(", ")}${selectedBatchNames.length > 4 ? "..." : ""}`}
                    </p>
                </div>

                <div className="mb-6 rounded-lg border p-4">
                    <div className="mb-3 flex items-center justify-between gap-2 text-sm font-medium text-foreground">
                        <div className="flex items-center gap-2">
                            <Filter className="h-4 w-4" />
                            Schedule Filters
                        </div>
                        <Button type="button" variant="ghost" className="h-8" onClick={handleResetFilters}>
                            Reset Filters
                        </Button>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
                        <Input
                            value={searchFilter}
                            onChange={(event) => setSearchFilter(event.target.value)}
                            placeholder="Search subject, teacher, room..."
                            className="h-10"
                        />
                        <select
                            value={batchFilter}
                            onChange={(event) => setBatchFilter(event.target.value)}
                            className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                        >
                            <option value="all">All Batches</option>
                            {sortedBatches.map((batch) => (
                                <option key={batch.id} value={batch.id}>
                                    {batch.name} sem {batch.semester}
                                </option>
                            ))}
                        </select>
                        <select
                            value={subjectFilter}
                            onChange={(event) => setSubjectFilter(event.target.value)}
                            className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                        >
                            <option value="all">All Subjects</option>
                            {sortedSubjects.map((subject) => (
                                <option key={subject.id} value={subject.id}>
                                    {subject.name}
                                </option>
                            ))}
                        </select>
                        <select
                            value={dayFilter}
                            onChange={(event) => setDayFilter(event.target.value)}
                            className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                        >
                            <option value="all">All Days</option>
                            {dayFilterOptions.map((day) => (
                                <option key={day} value={day}>
                                    {formatDay(day)}
                                </option>
                            ))}
                        </select>
                        <select
                            value={roomTypeFilter}
                            onChange={(event) => setRoomTypeFilter(event.target.value as "all" | "lab" | "classroom")}
                            className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                        >
                            <option value="all">All Rooms</option>
                            <option value="classroom">Classrooms Only</option>
                            <option value="lab">Labs Only</option>
                        </select>
                    </div>
                </div>

                <div>
                    <Card className="sticky top-8 border-border/60 shadow-sm">
                        <CardHeader className="pb-3">
                            <CardTitle className="text-lg font-semibold tracking-tight">
                                Timetable Entries ({filteredTimetables.length})
                            </CardTitle>
                        </CardHeader>

                        <CardContent className="overflow-auto p-0">
                            {isLoading ? (
                                <div className="py-10 text-center text-sm text-muted-foreground">
                                    Loading timetable...
                                </div>
                            ) : filteredTimetables.length === 0 ? (
                                <div className="py-10 text-center text-sm text-muted-foreground">
                                    {hasActiveFilters
                                        ? "No entries match current filters. Try resetting filters."
                                        : "No timetable entries available"}
                                </div>
                            ) : (
                                <table className="w-full min-w-[900px] border-collapse text-sm">
                                    <thead className="sticky top-0 z-10 bg-background/80 backdrop-blur border-b">
                                        <tr className="text-left text-xs uppercase tracking-wider text-muted-foreground">
                                            <th className="px-4 py-3 font-medium">Day / Time</th>
                                            {visibleTimeColumns.map((slot) => (
                                                <th key={slot} className="px-4 py-3 font-medium">
                                                    {formatTimeRangeLabel(slot)}
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>

                                    <tbody>
                                        {visibleDayRows.map((day) => (
                                            <tr
                                                key={day}
                                                className="border-b transition-colors hover:bg-muted/30"
                                            >
                                                <td className="px-4 py-4 font-medium text-foreground align-top">
                                                    {formatDay(day)}
                                                </td>

                                                {visibleTimeColumns.map((slotLabel) => {
                                                    const cellEntries =
                                                        timetableCellMap.get(`${day}::${slotLabel}`) ?? [];

                                                    return (
                                                        <td
                                                            key={`${day}-${slotLabel}`}
                                                            className="px-3 py-3 align-top"
                                                        >
                                                            {cellEntries.length === 0 ? (
                                                                <div className="text-xs text-muted-foreground/60 italic">
                                                                    No entry
                                                                </div>
                                                            ) : (
                                                                <div className="space-y-2">
                                                                    {cellEntries.map((entry) => {
                                                                        const batch = batchById.get(entry.batch_id);
                                                                        const subject = subjectById.get(entry.subject_id);
                                                                        const teacher = teacherById.get(entry.teacher_id);
                                                                        const room = roomById.get(entry.room_id);

                                                                        return (
                                                                            <button
                                                                                key={entry.id}
                                                                                type="button"
                                                                                onClick={() =>
                                                                                    handleSelectTimetable(entry)
                                                                                }
                                                                                className={`
                                  w-full text-left rounded-lg border p-3 transition-all
                                  hover:shadow-sm hover:-translate-y-[1px]
                                  ${selectedTimetableId === entry.id
                                                                                        ? "border-primary bg-primary/10 shadow-sm"
                                                                                        : "border-border/50 bg-background hover:bg-accent/50"
                                                                                    }
                                `}
                                                                            >
                                                                                <div className="font-semibold text-foreground leading-tight">
                                                                                    {subject?.name ??
                                                                                        `Subject #${entry.subject_id}`}
                                                                                </div>

                                                                                <div className="mt-1 text-xs text-muted-foreground">
                                                                                    {batch
                                                                                        ? `${batch.name} • Sem ${batch.semester}`
                                                                                        : `Batch #${entry.batch_id}`}
                                                                                </div>

                                                                                <div className="text-xs text-muted-foreground">
                                                                                    {teacher
                                                                                        ? `${teacher.abbreviation} — ${teacher.name}`
                                                                                        : `Teacher #${entry.teacher_id}`}
                                                                                </div>

                                                                                <div className="text-xs text-muted-foreground">
                                                                                    {room
                                                                                        ? `${room.name}${room.isLab ? " • Lab" : ""
                                                                                        }`
                                                                                        : `Room #${entry.room_id}`}
                                                                                </div>
                                                                            </button>
                                                                        );
                                                                    })}
                                                                </div>
                                                            )}
                                                        </td>
                                                    );
                                                })}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            )}
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}

export default TimeTable;