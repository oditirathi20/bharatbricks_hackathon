function SchemeCard({ scheme, onWhyEligible }) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card">
      <h3 className="text-lg font-semibold text-slate-900">{scheme.scheme_name}</h3>
      <p className="mt-2 text-sm text-slate-600">{scheme.benefit}</p>
      <button
        type="button"
        onClick={() => onWhyEligible(scheme)}
        className="mt-4 rounded-xl bg-teal-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-teal-700"
      >
        Why eligible
      </button>
    </article>
  )
}

export default SchemeCard
