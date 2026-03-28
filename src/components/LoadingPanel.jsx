function LoadingPanel({ title, subtitle }) {
  return (
    <div className="rounded-2xl border border-teal-200 bg-white/90 p-6 text-center shadow-card">
      <div className="mx-auto h-10 w-10 animate-spin rounded-full border-4 border-teal-100 border-t-teal-600" />
      <h3 className="mt-4 text-lg font-semibold text-slate-900">{title}</h3>
      <p className="mt-2 text-sm text-slate-600">{subtitle}</p>
    </div>
  )
}

export default LoadingPanel
