import { useState } from "react"

function VoiceQuestionCard({ question, value, onChange }) {
  const [isRecording, setIsRecording] = useState(false)
  const [transcript, setTranscript] = useState("")

  const handleSpeak = () => {
    if ("speechSynthesis" in window) {
      const utterance = new SpeechSynthesisUtterance(question.text)
      window.speechSynthesis.speak(utterance)
    }
  }

  const handleMockRecording = () => {
    setIsRecording(true)

    setTimeout(() => {
      const fakeTranscript = question.sampleTranscript || "sample response"
      setTranscript(`We heard: ${fakeTranscript}`)
      onChange(fakeTranscript)
      setIsRecording(false)
    }, 850)
  }

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card transition hover:-translate-y-0.5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <h3 className="text-base font-semibold text-slate-900">{question.text}</h3>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={handleSpeak}
            className="rounded-full bg-slate-100 px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:bg-slate-200"
          >
            Speak
          </button>
          <button
            type="button"
            onClick={handleMockRecording}
            className="rounded-full bg-teal-100 px-3 py-1.5 text-xs font-semibold text-teal-700 transition hover:bg-teal-200"
            disabled={isRecording}
          >
            {isRecording ? "Listening..." : "Mic"}
          </button>
        </div>
      </div>

      <div className="mt-4 space-y-2">
        {question.type === "select" ? (
          <select
            value={value}
            onChange={(event) => onChange(event.target.value)}
            className="w-full rounded-xl border border-slate-300 px-4 py-2.5 text-sm focus:border-teal-500 focus:outline-none"
          >
            <option value="">Select</option>
            {question.options?.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        ) : (
          <input
            type={question.type === "number" ? "number" : "text"}
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder={question.placeholder}
            className="w-full rounded-xl border border-slate-300 px-4 py-2.5 text-sm focus:border-teal-500 focus:outline-none"
          />
        )}

        <input
          type="text"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          placeholder="Text fallback input"
          className="w-full rounded-xl border border-slate-200 bg-slate-50 px-4 py-2 text-xs text-slate-700 focus:border-teal-500 focus:outline-none"
        />

        {transcript ? <p className="text-xs font-medium text-teal-700">{transcript}</p> : null}
      </div>
    </article>
  )
}

export default VoiceQuestionCard
