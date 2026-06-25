import { Page } from "../components/Layout";

/* Remaining stub — only the not-found fallback. Feature sections (notebooks,
 * dashboards, jobs) now have real pages in their own files. */

function Placeholder({ note }: { note: string }) {
  return (
    <div className="weft-card px-6 py-12 text-center">
      <p className="text-sm text-muted">{note}</p>
    </div>
  );
}

export function NotFoundPage() {
  return (
    <Page title="Not found" subtitle="That page does not exist.">
      <Placeholder note="Check the URL or pick a section from the left rail." />
    </Page>
  );
}
