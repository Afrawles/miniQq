import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import JobList from './JobList.tsx';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <JobList/>
  </StrictMode>,
)
