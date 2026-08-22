import { useEffect, useState } from "react";

interface Job {
    id: string;
    queue: string;
    payload: object;
    status: string;
    kind: string;
    max_attempts: number;
    created_at: string;
    claimed_at: string;
    run_at: string;
}

// TODO: add more data

interface JobsResp {
    jobs: Job[];
}
export default function JobList () {
    const [jobs, setJobs] = useState<Job[]>([]);
    const [error, setError] = useState<string>("");

    useEffect(() => {
        const controller = new AbortController();

        async function getJobs() {
            try {

                const resp = await fetch("/api/jobs", {signal: controller.signal});

                if (!resp.ok) {
                    throw new Error(`faield to fetch jobs, status: ${resp.status}`);
                }

                const data: JobsResp = await resp.json();
                setJobs(data.jobs)
            } catch (e) {
                    if (e instanceof Error && e.name === 'AbortError') {
                    return;
                }
                setError((e as Error).message)
            }
        }

        getJobs();

        return () => controller.abort();
    }, []);

    if (error) return <div>Error: {error}</div>;
    if (!jobs.length) return <div>No jobs</div>;

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
                    <th>Created At</th>
                </tr>
            </thead>
            <tbody>
                {
                    jobs.map(
                        (job) => (
                            <tr key={job.id}>
                                <td>{job.id}</td>
                                <td align="center">{job.queue}</td>
                                <td align="center">
                                    {typeof job.payload == "string" ? job.payload : JSON.stringify(job.payload)}
                                </td>
                                <td align="center">{job.status}</td>
                                <td align="center">{job.kind}</td>
                                <td align="center">{job.max_attempts}</td>
                                <td align="center">{job.created_at}</td>
                            </tr>
                ))}
            </tbody>
    </table>
    )
}
