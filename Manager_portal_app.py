import { useState } from 'react';
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableHeader, TableRow, TableHead, TableBody, TableCell } from "@/components/ui/table";
import { DatePicker } from "@/components/ui/datepicker";
import { Select, SelectItem } from "@/components/ui/select";

export default function ManagerPortal() {
    const [dateRange, setDateRange] = useState({ from: null, to: null });
    const [selectedEmployee, setSelectedEmployee] = useState(null);
    const [data, setData] = useState([]);

    // Placeholder for employees (this will later be fetched from API)
    const employees = [
        { id: 1, name: "John Doe" },
        { id: 2, name: "Jane Smith" },
    ];

    // Placeholder for call logs (this will later be fetched from API)
    const callLogs = [
        { employee: "John Doe", calls: 25, inbound: 10, outbound: 15, talkTime: "2h 30m" },
        { employee: "Jane Smith", calls: 30, inbound: 15, outbound: 15, talkTime: "3h 10m" },
    ];

    return (
        <div className="p-6">
            <h1 className="text-2xl font-bold mb-4">Manager Dashboard</h1>
            
            <Card className="mb-6">
                <CardContent className="flex flex-col md:flex-row justify-between items-center p-4">
                    <div>
                        <h2 className="text-lg font-semibold">Filter by Date</h2>
                        <DatePicker
                            selected={dateRange.from}
                            onChange={(date) => setDateRange({ ...dateRange, from: date })}
                            placeholderText="Start Date"
                        />
                        <DatePicker
                            selected={dateRange.to}
                            onChange={(date) => setDateRange({ ...dateRange, to: date })}
                            placeholderText="End Date"
                        />
                    </div>
                    <Select onValueChange={setSelectedEmployee} placeholder="Select Employee">
                        {employees.map(emp => (
                            <SelectItem key={emp.id} value={emp.name}>{emp.name}</SelectItem>
                        ))}
                    </Select>
                    <Button>Apply Filters</Button>
                </CardContent>
            </Card>

            <Card>
                <CardContent>
                    <h2 className="text-lg font-semibold mb-4">Call Logs</h2>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Employee</TableHead>
                                <TableHead>Total Calls</TableHead>
                                <TableHead>Inbound</TableHead>
                                <TableHead>Outbound</TableHead>
                                <TableHead>Talk Time</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {callLogs.filter(log => !selectedEmployee || log.employee === selectedEmployee).map((log, index) => (
                                <TableRow key={index}>
                                    <TableCell>{log.employee}</TableCell>
                                    <TableCell>{log.calls}</TableCell>
                                    <TableCell>{log.inbound}</TableCell>
                                    <TableCell>{log.outbound}</TableCell>
                                    <TableCell>{log.talkTime}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>
        </div>
    );
}
