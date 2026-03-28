import { useMemo, useState } from "react"
import { Navigate, useNavigate } from "react-router-dom"
import LoadingPanel from "../components/LoadingPanel"
import VoiceQuestionCard from "../components/VoiceQuestionCard"
import { useAppContext } from "../context/useAppContext"
import { QUESTIONS_BY_CATEGORY, SUPPORT_CATEGORIES } from "../data/questions"
import { getEligibilityResults, registerCitizen } from "../services/api"
import { buildConfirmationSummary, buildProfileFromAnswers } from "../utils/profile"

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

  const questions = useMemo(() => QUESTIONS_BY_CATEGORY[selectedCategory] || [], [selectedCategory])
  const summary = useMemo(() => buildConfirmationSummary(answers), [answers])

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
      <div className="mx-auto max-w-5xl px-4 py-10 sm:px-6 lg:px-8">
        <section className="rounded-3xl border border-slate-200 bg-white p-7 shadow-card sm:p-10">
          <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h1 className="text-3xl font-bold text-slate-900">Adaptive Onboarding</h1>
              <p className="mt-1 text-sm text-slate-600">Step {step} of 3</p>
            </div>
            <button
              type="button"
              onClick={() => navigate("/")}
              className="rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
            >
              Exit
            </button>
          </div>

          {step === 1 ? (
            <div>
              <h2 className="text-xl font-semibold text-slate-900">What kind of support are you looking for?</h2>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
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
                        ? "border-teal-500 bg-teal-50 text-teal-700"
                        : "border-slate-200 bg-white text-slate-700 hover:border-teal-300"
                    }`}
                  >
                    {categoryOption.label}
                  </button>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setStep(2)}
                disabled={!selectedCategory}
                className="mt-6 rounded-xl bg-teal-600 px-6 py-3 text-sm font-semibold text-white transition enabled:hover:bg-teal-700 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Continue
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
                  className="rounded-xl border border-slate-300 px-5 py-2.5 text-sm font-semibold text-slate-700"
                >
                  Back
                </button>
                <button
                  type="button"
                  onClick={() => setStep(3)}
                  className="rounded-xl bg-teal-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-teal-700"
                >
                  Review Answers
                </button>
              </div>
            </div>
          ) : null}

          {step === 3 ? (
            <div>
              <h2 className="text-xl font-semibold text-slate-900">We understood the following:</h2>
              <div className="mt-4 space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-700">
                <p>
                  <span className="font-semibold">Income:</span> {summary.income}
                </p>
                <p>
                  <span className="font-semibold">Occupation:</span> {summary.occupation}
                </p>
                <p>
                  <span className="font-semibold">Key details:</span> {summary.keyDetails}
                </p>
              </div>

              {isSubmitting ? (
                <div className="mt-6">
                  <LoadingPanel
                    title="Computing eligibility"
                    subtitle="Sending profile to backend and loading eligible schemes..."
                  />
                </div>
              ) : (
                <div className="mt-6 flex flex-wrap gap-3">
                  <button
                    type="button"
                    onClick={() => setStep(2)}
                    className="rounded-xl border border-slate-300 px-5 py-2.5 text-sm font-semibold text-slate-700"
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    onClick={handleSubmit}
                    className="rounded-xl bg-teal-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-teal-700"
                  >
                    Confirm
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
