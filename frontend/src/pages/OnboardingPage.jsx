import { useMemo, useState } from "react"
import { Navigate, useNavigate } from "react-router-dom"
import LoadingPanel from "../components/LoadingPanel"
import VoiceQuestionCard from "../components/VoiceQuestionCard"
import { useAppContext } from "../context/useAppContext"
import { QUESTIONS_BY_CATEGORY, SUPPORT_CATEGORIES } from "../data/questions"
import { useTranslation } from "../i18n/useTranslation"
import { getEligibilityResults, registerCitizen } from "../services/api"
import { buildConfirmationSummary, buildProfileFromAnswers } from "../utils/profile"

const VALUE_TO_TRANSLATION_KEY = {
  yes: "common.yes",
  no: "common.no",
  general: "onboarding.options.general",
  obc: "onboarding.options.obc",
  sc: "onboarding.options.sc",
  st: "onboarding.options.st",
  student: "onboarding.options.student",
  child: "onboarding.options.child",
}

function OnboardingPage() {
  const navigate = useNavigate()
  const {
    citizenId,
    selectedCategory,
    setSelectedCategory,
    answers,
    setAnswers,
    setProfile,
    setResults,
  } = useAppContext()

  const [step, setStep] = useState(1)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const { t } = useTranslation()

  const questions = useMemo(
    () =>
      (QUESTIONS_BY_CATEGORY[selectedCategory] || []).map((question) => ({
        ...question,
        text: t(question.textKey),
        placeholder: question.placeholderKey ? t(question.placeholderKey) : "",
        sampleTranscript: question.sampleTranscriptKey ? t(question.sampleTranscriptKey) : "",
        options:
          question.options?.map((option) => ({
            ...option,
            label: t(option.labelKey),
          })) || [],
      })),
    [selectedCategory, t],
  )
  const summary = useMemo(() => buildConfirmationSummary(answers), [answers])

  const formatSummaryValue = (value) => {
    if (!value) {
      return t("common.notProvided")
    }

    const normalized = String(value).trim().toLowerCase()
    const translationKey = VALUE_TO_TRANSLATION_KEY[normalized]

    return translationKey ? t(translationKey) : value
  }

  const keyDetailsText =
    summary.keyDetailsList?.length
      ? summary.keyDetailsList.map((item) => formatSummaryValue(item)).join(" | ")
      : t("common.notProvided")

  if (!citizenId) {
    return <Navigate to="/login" replace />
  }

  const updateAnswer = (key, value) => {
    setAnswers((previous) => ({
      ...previous,
      [key]: value,
    }))
  }

  const handleSubmit = async () => {
    setIsSubmitting(true)

    const payload = buildProfileFromAnswers({ citizenId, answers })
    setProfile(payload)

    await registerCitizen(payload)
    const resultsResponse = await getEligibilityResults(citizenId, payload)

    setResults(resultsResponse.schemes)
    setIsSubmitting(false)
    navigate("/dashboard")
  }

  return (
    <main className="page-shell">
      <div className="page-container max-w-6xl">
        <section className="section-card p-7 sm:p-10">
          <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h1 className="page-title text-3xl">{t("onboarding.title")}</h1>
              <p className="page-subtitle mt-1">{t("onboarding.stepText", { step, total: 3 })}</p>
            </div>
            <button
              type="button"
              onClick={() => navigate("/")}
              className="btn btn-ghost"
            >
              {t("onboarding.exit")}
            </button>
          </div>

          {step === 1 ? (
            <div>
              <h2 className="text-2xl font-semibold text-slate-900">{t("onboarding.supportPrompt")}</h2>
              <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {SUPPORT_CATEGORIES.map((categoryOption) => (
                  <button
                    key={categoryOption.id}
                    type="button"
                    onClick={() => {
                      setSelectedCategory(categoryOption.id)
                      setAnswers({})
                    }}
                    className={`rounded-2xl border px-4 py-4 text-left text-sm font-semibold transition ${
                      selectedCategory === categoryOption.id
                        ? "border-teal-500 bg-teal-50 text-teal-800 shadow-sm"
                        : "border-slate-200 bg-white text-slate-700 hover:border-teal-300 hover:bg-teal-50/40"
                    }`}
                  >
                    {t(categoryOption.labelKey)}
                  </button>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setStep(2)}
                disabled={!selectedCategory}
                className="btn btn-primary mt-6 px-6 py-3.5 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {t("common.continue")}
              </button>
            </div>
          ) : null}

          {step === 2 ? (
            <div>
              <div className="grid gap-4">
                {questions.map((question) => (
                  <VoiceQuestionCard
                    key={question.id}
                    question={question}
                    value={answers[question.id] || ""}
                    onChange={(value) => updateAnswer(question.id, value)}
                  />
                ))}
              </div>

              <div className="mt-6 flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={() => setStep(1)}
                  className="btn btn-ghost"
                >
                  {t("common.back")}
                </button>
                <button
                  type="button"
                  onClick={() => setStep(3)}
                  className="btn btn-primary"
                >
                  {t("onboarding.reviewAnswers")}
                </button>
              </div>
            </div>
          ) : null}

          {step === 3 ? (
            <div>
              <h2 className="text-2xl font-semibold text-slate-900">{t("onboarding.summaryTitle")}</h2>
              <div className="panel-muted mt-4 space-y-3 p-5 text-sm text-slate-700">
                <p>
                  <span className="font-semibold">{t("onboarding.incomeLabel")}:</span> {formatSummaryValue(summary.income)}
                </p>
                <p>
                  <span className="font-semibold">{t("onboarding.occupationLabel")}:</span> {formatSummaryValue(summary.occupation)}
                </p>
                <p>
                  <span className="font-semibold">{t("onboarding.keyDetailsLabel")}:</span> {keyDetailsText}
                </p>
              </div>

              {isSubmitting ? (
                <div className="mt-6">
                  <LoadingPanel
                    title={t("onboarding.loadingTitle")}
                    subtitle={t("onboarding.loadingSubtitle")}
                  />
                </div>
              ) : (
                <div className="mt-6 flex flex-wrap gap-3">
                  <button
                    type="button"
                    onClick={() => setStep(2)}
                    className="btn btn-ghost"
                  >
                    {t("common.edit")}
                  </button>
                  <button
                    type="button"
                    onClick={handleSubmit}
                    className="btn btn-primary"
                  >
                    {t("common.confirm")}
                  </button>
                </div>
              )}
            </div>
          ) : null}
        </section>
      </div>
    </main>
  )
}

export default OnboardingPage
