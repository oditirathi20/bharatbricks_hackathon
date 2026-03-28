import { useTranslation } from "../i18n/useTranslation"

function SchemeCard({ scheme, onWhyEligible }) {
  const { t } = useTranslation()

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card transition hover:-translate-y-0.5 hover:shadow-lg">
      <p className="eyebrow">{t("schemeCard.eligibleScheme")}</p>
      <h3 className="mt-2 text-lg font-semibold text-slate-900">{scheme.scheme_name}</h3>
      <p className="mt-2 text-sm leading-relaxed text-slate-600">{scheme.benefit}</p>
      <button
        type="button"
        onClick={() => onWhyEligible(scheme)}
        className="btn btn-primary mt-4"
      >
        {t("schemeCard.whyEligible")}
      </button>
    </article>
  )
}

export default SchemeCard
