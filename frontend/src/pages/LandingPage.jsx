import { useNavigate } from "react-router-dom"
import LanguageSwitcher from "../components/LanguageSwitcher"
import { useAppContext } from "../context/useAppContext"
import { useTranslation } from "../i18n/useTranslation"

function LandingPage() {
  const navigate = useNavigate()
  const { resetJourney } = useAppContext()
  const { t } = useTranslation()

  const howItWorks = [
    {
      id: "1",
      title: t("landing.step1Title"),
      description: t("landing.step1Description"),
    },
    {
      id: "2",
      title: t("landing.step2Title"),
      description: t("landing.step2Description"),
    },
    {
      id: "3",
      title: t("landing.step3Title"),
      description: t("landing.step3Description"),
    },
  ]

  return (
    <main className="page-shell">
      <div className="page-container max-w-6xl">
        <div className="top-strip">
          <LanguageSwitcher />
        </div>

        {/* 60/40 split keeps the primary message and CTA dominant while preserving guidance on the right. */}
        <section className="section-card mt-8 grid overflow-hidden border-gray-200 p-7 shadow-2xl sm:p-10 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] lg:gap-10">
          <div>
            <p className="chip chip-brand">{t("landing.badge")}</p>
            <h1 className="page-title mt-5">{t("landing.title")}</h1>
            <p className="mt-3 text-base font-semibold text-slate-700 sm:text-lg">
              {t("landing.subtitle")}
            </p>
            <p className="mt-3 max-w-2xl text-base leading-relaxed text-slate-600 sm:text-lg">
              {t("landing.description")}
            </p>

            {/* Primary CTA uses gradient, motion, and iconography to clearly signal the next action. */}
            <div className="mt-9 flex flex-wrap gap-3">
              <button
                type="button"
                onClick={() => {
                  resetJourney()
                  navigate("/login")
                }}
                className="btn btn-primary px-8 py-4 text-base"
              >
                {t("landing.getStarted")} <span aria-hidden="true" className="text-lg">-&gt;</span>
              </button>
            </div>

            <p className="mt-3 text-xs font-medium tracking-wide text-slate-500 sm:text-sm">
              {t("landing.trustIndicator")}
            </p>
          </div>

          {/* Step cards provide a premium, scannable explanation of the workflow. */}
          <aside className="panel-muted mt-8 p-5 lg:mt-0">
            <p className="eyebrow">{t("landing.howItWorks")}</p>
            <div className="mt-3 space-y-3">
              {howItWorks.map((step) => (
                <article key={step.id} className="step-card">
                  <div className="flex items-start gap-3">
                    <span className="step-icon" aria-hidden="true">{step.id}</span>
                    <div>
                      <h3 className="text-sm font-bold text-slate-900">{step.title}</h3>
                      <p className="mt-1 text-xs leading-relaxed text-slate-600 sm:text-sm">{step.description}</p>
                    </div>
                  </div>
                </article>
              ))}
            </div>
          </aside>
        </section>
      </div>
    </main>
  )
}

export default LandingPage
