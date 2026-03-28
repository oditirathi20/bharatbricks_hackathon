import { useNavigate } from "react-router-dom"
import LanguageSelector from "../components/LanguageSelector"
import { useAppContext } from "../context/useAppContext"

function LandingPage() {
  const navigate = useNavigate()
  const { language, setLanguage, resetJourney } = useAppContext()

  return (
    <main className="page-shell">
      <div className="mx-auto max-w-5xl px-4 py-10 sm:px-6 lg:px-8">
        <div className="flex justify-end">
          <LanguageSelector value={language} onChange={setLanguage} />
        </div>

        <section className="mt-8 overflow-hidden rounded-3xl border border-slate-200 bg-white/90 p-7 shadow-card sm:p-10">
          <p className="inline-flex rounded-full bg-amber-100 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-amber-700">
            AI-powered citizen support
          </p>
          <h1 className="mt-5 text-4xl font-bold leading-tight text-slate-900 sm:text-5xl">
            Adhikar-Aina: Know Your Rights
          </h1>
          <p className="mt-4 max-w-2xl text-base text-slate-600 sm:text-lg">
            Discover government schemes you are eligible for through guided questions, multilingual support, and instant eligibility insights.
          </p>

          <div className="mt-8 flex flex-wrap gap-3">
            <button
              type="button"
              onClick={() => {
                resetJourney()
                navigate("/login")
              }}
              className="rounded-xl bg-teal-600 px-6 py-3 text-sm font-semibold text-white transition hover:bg-teal-700 sm:text-base"
            >
              Get Started
            </button>
          </div>
        </section>
      </div>
    </main>
  )
}

export default LandingPage
