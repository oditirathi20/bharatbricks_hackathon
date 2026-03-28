import { useState } from "react"
import { useLanguage } from "../i18n/useLanguage"
import { useTranslation } from "../i18n/useTranslation"
import { voiceConfig } from "../i18n/voiceConfig"

function VoiceQuestionCard({ question, value, onChange }) {
  const [isRecording, setIsRecording] = useState(false)
  const [transcript, setTranscript] = useState("")
  const { language } = useLanguage()
  const { t } = useTranslation()

  const handleSpeak = () => {
    if ("speechSynthesis" in window) {
      const utterance = new SpeechSynthesisUtterance(question.text)
      utterance.lang = voiceConfig[language]
      window.speechSynthesis.speak(utterance)
    }
  }

  const handleMockRecording = () => {
    setIsRecording(true)

    setTimeout(() => {
      const fakeTranscript = question.sampleTranscript || t("onboarding.transcripts.sampleResponse")
      setTranscript(t("onboarding.transcripts.heardPrefix", { text: fakeTranscript }))
      onChange(fakeTranscript)
      setIsRecording(false)
    }, 850)
  }

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card transition hover:-translate-y-0.5 hover:shadow-lg">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <h3 className="text-[1.01rem] font-semibold leading-snug text-slate-900">{question.text}</h3>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={handleSpeak}
            className="btn btn-ghost rounded-full px-3 py-2 text-xs"
          >
            {t("voice.speaker")}
          </button>
          <button
            type="button"
            onClick={handleMockRecording}
            className="btn btn-soft rounded-full px-3 py-2 text-xs"
            disabled={isRecording}
          >
            {isRecording ? t("voice.listening") : t("voice.mic")}
          </button>
        </div>
      </div>

      <div className="mt-4 space-y-2.5">
        {question.type === "select" ? (
          <select
            value={value}
            onChange={(event) => onChange(event.target.value)}
            className="select-control"
          >
            <option value="">{t("common.select")}</option>
            {question.options?.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        ) : (
          <input
            type={question.type === "number" ? "number" : "text"}
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder={question.placeholder}
            className="input-control"
          />
        )}

        <input
          type="text"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          placeholder={t("common.textFallbackInput")}
          className="input-control bg-slate-50 text-xs"
        />

        {transcript ? <p className="chip chip-brand text-[0.72rem] normal-case tracking-normal">{transcript}</p> : null}
      </div>
    </article>
  )
}

export default VoiceQuestionCard
