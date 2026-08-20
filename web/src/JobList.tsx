import { useEffect, useState } from "react";

interface Job {
    id: string;
    queue: string;
    payload: string;
    status: string;
    kind: string;
    max_attempts: number;
}

// TODO: add more data

interface JobsResp {
    jobs: Job[];
}
export default function JobList () {
    const [jobs, setJobs] = useState<Job[]>([]);
    useEffect(() => {
        const controller = new AbortController();

        async function getJobs() {
            const resp = await fetch("/api/jobs", {signal: controller.signal});

            if (!resp.ok) {
                throw new Error(`faield to fetch jobs, status: ${resp.status}`);
            }

            const data: JobsResp = await resp.json();
            setJobs(data.jobs)
        }

        getJobs();

        return () => controller.abort();
    }, []);

    return (
    <table cellPadding="5" cellSpacing="10">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Queue</th>
                    <th>Payload</th>
                    <th>Status</th>
                    <th>Type</th>
                    <th>Max Attempts</th>
                </tr>
            </thead>
            <tbody>
                {
                    jobs.map(
                        (job) => (
                            <tr key={job.id}>
                                <td>{job.id}</td>
                                <td align="center">{job.queue}</td>
                                <td align="center">{job.payload}</td>
                                <td align="center">{job.status}</td>
                                <td align="center">{job.kind}</td>
                                <td align="center">{job.max_attempts}</td>
                            </tr>
                ))}
            </tbody>
    </table>
    )
}
