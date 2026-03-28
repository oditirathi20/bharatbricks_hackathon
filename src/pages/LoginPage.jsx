import { useState } from "react"
import { useNavigate } from "react-router-dom"
import LanguageSelector from "../components/LanguageSelector"
import { useAppContext } from "../context/useAppContext"

function LoginPage() {
  const navigate = useNavigate()
  const { language, setLanguage, setCitizenId } = useAppContext()
  const [localCitizenId, setLocalCitizenId] = useState("")

  const handleContinue = () => {
    if (!localCitizenId.trim()) {
      return
    }

    setCitizenId(localCitizenId.trim())
    navigate("/onboarding")
  }

  return (
    <main className="page-shell">
      <div className="mx-auto max-w-3xl px-4 py-10 sm:px-6 lg:px-8">
        <section className="rounded-3xl border border-slate-200 bg-white p-7 shadow-card sm:p-10">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h1 className="text-3xl font-bold text-slate-900">Citizen Login</h1>
            <LanguageSelector value={language} onChange={setLanguage} />
          </div>

          <p className="mt-3 text-sm text-slate-600">Enter your Citizen ID (simulated Aadhaar) to continue.</p>

          <div className="mt-6 space-y-3">
            <label htmlFor="citizenId" className="text-sm font-semibold text-slate-700">
              Citizen ID
            </label>
            <input
              id="citizenId"
              value={localCitizenId}
              onChange={(event) => setLocalCitizenId(event.target.value)}
              placeholder="e.g. 1234-5678-9012"
              className="w-full rounded-xl border border-slate-300 px-4 py-3 text-sm focus:border-teal-500 focus:outline-none"
            />
          </div>

          <button
            type="button"
            onClick={handleContinue}
            className="mt-7 w-full rounded-xl bg-teal-600 px-6 py-3 text-sm font-semibold text-white transition hover:bg-teal-700 sm:w-auto"
          >
            Continue
          </button>
        </section>
      </div>
    </main>
  )
}

export default LoginPage
