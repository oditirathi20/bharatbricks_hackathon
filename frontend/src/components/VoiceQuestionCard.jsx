import { useEffect, useRef, useState } from "react"
import { useLanguage } from "../i18n/useLanguage"
import { useTranslation } from "../i18n/useTranslation"
import { voiceConfig } from "../i18n/voiceConfig"
import { requestTtsAudio } from "../services/api"

function VoiceQuestionCard({ question, value, onChange }) {
  const [isRecording, setIsRecording] = useState(false)
  const [isSpeaking, setIsSpeaking] = useState(false)
  const [transcript, setTranscript] = useState("")
  const audioRef = useRef(null)
  const { language } = useLanguage()
  const { t } = useTranslation()

  useEffect(
    () => () => {
      if (audioRef.current) {
        audioRef.current.pause()
        audioRef.current = null
      }
      window.speechSynthesis?.cancel()
    },
    [],
  )

  const fallbackBrowserSpeak = (text) => {
    if ("speechSynthesis" in window) {
      const utterance = new SpeechSynthesisUtterance(text)
      utterance.lang = voiceConfig[language] || voiceConfig.en
      window.speechSynthesis.speak(utterance)
    }
  }

  const speakText = async (text) => {
    setIsSpeaking(true)

    try {
      const audioBlob = await requestTtsAudio(text, language)

      if (audioRef.current) {
        audioRef.current.pause()
        audioRef.current = null
      }

      const audioUrl = URL.createObjectURL(audioBlob)
      const audio = new Audio(audioUrl)
      audioRef.current = audio

      audio.onended = () => {
        URL.revokeObjectURL(audioUrl)
        setIsSpeaking(false)
      }

      audio.onerror = () => {
        URL.revokeObjectURL(audioUrl)
        setIsSpeaking(false)
      }

      await audio.play()
    } catch {
      setIsSpeaking(false)
      fallbackBrowserSpeak(text)
    }
  }

  const handleSpeak = () => speakText(question.text)

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
            disabled={isSpeaking}
            aria-label={t("voice.speaker")}
            title={t("voice.speaker")}
          >
            <span aria-hidden="true" className="inline-flex items-center justify-center">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M3 10V14H7L12 19V5L7 10H3Z" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                <path d="M16 9C17.2 10.2 17.2 13.8 16 15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                <path d="M19 6C21.8 8.8 21.8 15.2 19 18" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              </svg>
            </span>
          </button>
          <button
            type="button"
            onClick={handleMockRecording}
            className="btn btn-soft rounded-full px-3 py-2 text-xs"
            disabled={isRecording}
            aria-label={t("voice.mic")}
            title={isRecording ? t("voice.listening") : t("voice.mic")}
          >
            <span aria-hidden="true" className="inline-flex items-center justify-center">
              {isRecording ? (
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <rect x="9" y="3" width="6" height="12" rx="3" stroke="currentColor" strokeWidth="1.8" />
                  <path d="M5 11C5 14.3137 7.68629 17 11 17H13C16.3137 17 19 14.3137 19 11" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                  <path d="M12 17V21" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                  <path d="M9 21H15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                </svg>
              ) : (
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <rect x="9" y="3" width="6" height="12" rx="3" stroke="currentColor" strokeWidth="1.8" />
                  <path d="M5 11C5 14.3137 7.68629 17 11 17H13C16.3137 17 19 14.3137 19 11" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                  <path d="M12 17V21" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                  <path d="M9 21H15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                </svg>
              )}
            </span>
          </button>
        </div>
      </div>

      <div className="mt-4 space-y-2.5">
        <input
          type={question.type === "number" ? "number" : "text"}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          placeholder={question.placeholder || t("common.textFallbackInput")}
          className="input-control"
        />

        {transcript ? <p className="chip chip-brand text-[0.72rem] normal-case tracking-normal">{transcript}</p> : null}
      </div>
    </article>
  )
}

export default VoiceQuestionCard
