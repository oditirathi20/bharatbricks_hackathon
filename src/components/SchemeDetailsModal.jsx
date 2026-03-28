function SchemeDetailsModal({ scheme, onClose }) {
  if (!scheme) {
    return null
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/55 p-4">
      <div className="max-h-[90vh] w-full max-w-xl overflow-auto rounded-2xl bg-white p-6 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-xl font-bold text-slate-900">{scheme.scheme_name}</h3>
            <p className="mt-1 text-sm text-slate-600">{scheme.benefit}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-200"
          >
            Close
          </button>
        </div>

        <div className="mt-6 space-y-4">
          <section>
            <h4 className="text-sm font-semibold uppercase tracking-wide text-slate-500">Eligibility reason</h4>
            <p className="mt-1 text-sm text-slate-700">{scheme.reason || "Returned by backend eligibility engine."}</p>
          </section>
          <section>
            <h4 className="text-sm font-semibold uppercase tracking-wide text-slate-500">Required documents</h4>
            <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
              {(scheme.requiredDocuments || ["Aadhaar", "Income certificate", "Bank details"]).map((doc) => (
                <li key={doc}>{doc}</li>
              ))}
            </ul>
          </section>
        </div>
      </div>
    </div>
  )
}

export default SchemeDetailsModal
