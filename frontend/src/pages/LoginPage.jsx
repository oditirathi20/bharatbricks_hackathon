import { useState } from "react"
import { useNavigate } from "react-router-dom"
import LanguageSwitcher from "../components/LanguageSwitcher"
import { useAppContext } from "../context/useAppContext"
import { useTranslation } from "../i18n/useTranslation"

function LoginPage() {
  const navigate = useNavigate()
  const { setCitizenId } = useAppContext()
  const { t } = useTranslation()
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
      <div className="page-container max-w-4xl">
        <section className="section-card p-7 sm:p-10">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h1 className="page-title text-3xl">{t("login.title")}</h1>
            <LanguageSwitcher />
          </div>

          <p className="page-subtitle mt-3">{t("login.subtitle")}</p>

          <div className="mt-6 space-y-3">
            <label htmlFor="citizenId" className="field-label">
              {t("login.citizenIdLabel")}
            </label>
            <input
              id="citizenId"
              value={localCitizenId}
              onChange={(event) => setLocalCitizenId(event.target.value)}
              placeholder={t("login.citizenIdPlaceholder")}
              className="input-control"
            />
          </div>

          <button
            type="button"
            onClick={handleContinue}
            className="btn btn-primary mt-7 w-full px-6 py-3.5 sm:w-auto"
          >
            {t("login.continue")}
          </button>
        </section>
      </div>
    </main>
  )
}

export default LoginPage
