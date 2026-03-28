import { useMemo, useState } from "react"
import { Navigate, useNavigate } from "react-router-dom"
import SchemeCard from "../components/SchemeCard"
import SchemeDetailsModal from "../components/SchemeDetailsModal"
import { useAppContext } from "../context/useAppContext"
import { ALL_SCHEMES } from "../data/schemes"
import { useTranslation } from "../i18n/useTranslation"

const FILTERS = ["all", "agriculture", "business", "education", "housing"]

function DashboardPage() {
  const navigate = useNavigate()
  const { citizenId, selectedCategory, results, resetJourney } = useAppContext()
  const [activeFilter, setActiveFilter] = useState(selectedCategory || "all")
  const [selectedScheme, setSelectedScheme] = useState(null)
  const { t } = useTranslation()

  const schemeDetailsByName = useMemo(() => {
    const map = {}

    ALL_SCHEMES.forEach((scheme) => {
      map[scheme.scheme_name] = scheme
    })

    return map
  }, [])

  const eligibleSchemes = useMemo(
    () =>
      results.map((scheme) => ({
        ...scheme,
        requiredDocuments: schemeDetailsByName[scheme.scheme_name]?.requiredDocuments || [
          "Aadhaar",
          "Address proof",
          "Bank details",
        ],
      })),
    [results, schemeDetailsByName],
  )

  const eligibleSet = useMemo(
    () => new Set(eligibleSchemes.map((scheme) => scheme.scheme_name)),
    [eligibleSchemes],
  )

  const filteredSchemes = useMemo(() => {
    if (activeFilter === "all") {
      return ALL_SCHEMES
    }

    return ALL_SCHEMES.filter((scheme) => scheme.category === activeFilter)
  }, [activeFilter])

  if (!citizenId) {
    return <Navigate to="/login" replace />
  }

  return (
    <main className="page-shell">
      <div className="page-container max-w-7xl">
        <section className="section-card p-7 sm:p-10">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h1 className="page-title text-3xl">{t("dashboard.title")}</h1>
              <p className="page-subtitle mt-1">{t("dashboard.citizenIdLabel")}: {citizenId}</p>
            </div>
            <button
              type="button"
              onClick={() => {
                resetJourney()
                navigate("/")
              }}
              className="btn btn-ghost"
            >
              {t("dashboard.newCheck")}
            </button>
          </div>

          <div className="mt-7">
            <h2 className="text-2xl font-semibold text-slate-900">{t("dashboard.eligibleSchemes")}</h2>
            {eligibleSchemes.length === 0 ? (
              <p className="mt-3 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
                {t("dashboard.noEligibleMessage")}
              </p>
            ) : (
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                {eligibleSchemes.map((scheme) => (
                  <SchemeCard key={scheme.scheme_name} scheme={scheme} onWhyEligible={setSelectedScheme} />
                ))}
              </div>
            )}
          </div>

          <div className="mt-10">
            <h2 className="text-2xl font-semibold text-slate-900">{t("dashboard.exploreMoreSchemes")}</h2>
            <div className="mt-4 flex flex-wrap gap-2">
              {FILTERS.map((filter) => (
                <button
                  key={filter}
                  type="button"
                  onClick={() => setActiveFilter(filter)}
                  className={`rounded-full px-4 py-2 text-xs font-semibold uppercase tracking-wide transition ${
                    activeFilter === filter
                      ? "bg-slate-900 text-white shadow-sm"
                      : "bg-slate-100 text-slate-700 hover:bg-slate-200"
                  }`}
                >
                  {t(`dashboard.filter.${filter}`)}
                </button>
              ))}
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-2">
              {filteredSchemes.map((scheme) => {
                const isEligible = eligibleSet.has(scheme.scheme_name)

                return (
                  <article
                    key={scheme.id}
                    className={`rounded-2xl border p-4 ${
                      isEligible
                        ? "border-emerald-200 bg-emerald-50"
                        : "border-slate-200 bg-slate-100"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <h3 className="text-sm font-semibold leading-snug text-slate-900">{scheme.scheme_name}</h3>
                      <span
                        className={`rounded-full px-2.5 py-1 text-xs font-semibold ${
                          isEligible
                            ? "bg-emerald-200 text-emerald-800"
                            : "bg-slate-200 text-slate-600"
                        }`}
                      >
                        {isEligible ? t("dashboard.eligible") : t("dashboard.notEligible")}
                      </span>
                    </div>
                    <p className="mt-2 text-xs leading-relaxed text-slate-600">{scheme.benefit}</p>
                  </article>
                )
              })}
            </div>
          </div>
        </section>
      </div>

      <SchemeDetailsModal scheme={selectedScheme} onClose={() => setSelectedScheme(null)} />
    </main>
  )
}

export default DashboardPage
