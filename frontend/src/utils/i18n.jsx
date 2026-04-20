import { createContext, useContext, useState, useCallback, useEffect } from 'react'

const translations = {
  en: {
    'sidebar.chat': 'Chat',
    'sidebar.dashboard': 'Dashboard',
    'sidebar.schema': 'Schema Map',
    'sidebar.schedules': 'Schedules',
    'sidebar.files': 'Files',
    'sidebar.connectors': 'Connectors',
    'sidebar.audit': 'Audit Log',
    'sidebar.users': 'Users',
    'sidebar.newChat': 'New Chat',
    'sidebar.pinned': 'Pinned',
    'sidebar.recent': 'Recent',
    'sidebar.searchChats': 'Search chats...',
    'chat.placeholder': 'Ask about your Salesforce data...',
    'chat.placeholderFile': 'Ask about {file}...',
    'chat.welcome': 'Welcome, {name}',
    'chat.welcomeSub': 'Ask anything about your Salesforce data. Real-time queries, instant answers.',
    'chat.shiftEnter': 'Shift+Enter for new line · Attach CSV/XLSX',
    'chat.uploading': 'Uploading…',
    'chat.loggedInAs': 'Logged in as {name}',
    'dashboard.title': 'Dashboard',
    'login.title': 'Fyxo Chat',
    'login.subtitle': 'Sign in to continue',
    'login.username': 'Username',
    'login.password': 'Password',
    'login.signIn': 'Sign In',
    'login.signingIn': 'Signing in...',
    'actions.copy': 'Copy',
    'actions.download': 'Download CSV',
    'actions.pdf': 'Export PDF',
    'actions.email': 'Send as Email',
    'actions.schedule': 'Schedule this report',
    'actions.good': 'Good answer',
    'actions.bad': 'Bad answer',
    'common.loading': 'Loading…',
    'common.refresh': 'Refresh',
    'common.save': 'Save',
    'common.cancel': 'Cancel',
    'common.delete': 'Delete',
    'common.edit': 'Edit',
    'language.label': 'Language',
  },
  hi: {
    'sidebar.chat': 'चैट',
    'sidebar.dashboard': 'डैशबोर्ड',
    'sidebar.schema': 'स्कीमा मैप',
    'sidebar.schedules': 'शेड्यूल',
    'sidebar.files': 'फ़ाइलें',
    'sidebar.connectors': 'कनेक्टर',
    'sidebar.audit': 'ऑडिट लॉग',
    'sidebar.users': 'उपयोगकर्ता',
    'sidebar.newChat': 'नई चैट',
    'sidebar.pinned': 'पिन किए गए',
    'sidebar.recent': 'हाल के',
    'sidebar.searchChats': 'चैट खोजें...',
    'chat.placeholder': 'अपने Salesforce डेटा के बारे में पूछें...',
    'chat.placeholderFile': '{file} के बारे में पूछें...',
    'chat.welcome': 'स्वागत है, {name}',
    'chat.welcomeSub': 'अपने Salesforce डेटा के बारे में कुछ भी पूछें। रीयल-टाइम क्वेरी, त्वरित उत्तर।',
    'chat.shiftEnter': 'नई पंक्ति के लिए Shift+Enter · CSV/XLSX संलग्न करें',
    'chat.uploading': 'अपलोड हो रहा है…',
    'chat.loggedInAs': '{name} के रूप में लॉग इन',
    'dashboard.title': 'डैशबोर्ड',
    'login.title': 'Fyxo Chat',
    'login.subtitle': 'जारी रखने के लिए साइन इन करें',
    'login.username': 'उपयोगकर्ता नाम',
    'login.password': 'पासवर्ड',
    'login.signIn': 'साइन इन करें',
    'login.signingIn': 'साइन इन हो रहा है...',
    'actions.copy': 'कॉपी',
    'actions.download': 'CSV डाउनलोड',
    'actions.pdf': 'PDF निर्यात',
    'actions.email': 'ईमेल भेजें',
    'actions.schedule': 'रिपोर्ट शेड्यूल करें',
    'actions.good': 'अच्छा उत्तर',
    'actions.bad': 'खराब उत्तर',
    'common.loading': 'लोड हो रहा है…',
    'common.refresh': 'रीफ्रेश',
    'common.save': 'सहेजें',
    'common.cancel': 'रद्द करें',
    'common.delete': 'हटाएं',
    'common.edit': 'संपादित करें',
    'language.label': 'भाषा',
  },
  te: {
    'sidebar.chat': 'చాట్',
    'sidebar.dashboard': 'డాష్‌బోర్డ్',
    'sidebar.schema': 'స్కీమా మ్యాప్',
    'sidebar.schedules': 'షెడ్యూల్స్',
    'sidebar.files': 'ఫైల్స్',
    'sidebar.connectors': 'కనెక్టర్లు',
    'sidebar.audit': 'ఆడిట్ లాగ్',
    'sidebar.users': 'వినియోగదారులు',
    'sidebar.newChat': 'కొత్త చాట్',
    'sidebar.pinned': 'పిన్ చేసినవి',
    'sidebar.recent': 'ఇటీవలివి',
    'sidebar.searchChats': 'చాట్‌లను వెతకండి...',
    'chat.placeholder': 'మీ Salesforce డేటా గురించి అడగండి...',
    'chat.placeholderFile': '{file} గురించి అడగండి...',
    'chat.welcome': 'స్వాగతం, {name}',
    'chat.welcomeSub': 'మీ Salesforce డేటా గురించి ఏదైనా అడగండి. రియల్-టైమ్ ప్రశ్నలు, తక్షణ సమాధానాలు.',
    'chat.shiftEnter': 'కొత్త పంక్తికి Shift+Enter · CSV/XLSX జోడించండి',
    'chat.uploading': 'అప్‌లోడ్ అవుతోంది…',
    'chat.loggedInAs': '{name} గా లాగిన్',
    'dashboard.title': 'డాష్‌బోర్డ్',
    'login.title': 'Fyxo Chat',
    'login.subtitle': 'కొనసాగించడానికి సైన్ ఇన్ చేయండి',
    'login.username': 'వినియోగదారు పేరు',
    'login.password': 'పాస్‌వర్డ్',
    'login.signIn': 'సైన్ ఇన్ చేయండి',
    'login.signingIn': 'సైన్ ఇన్ అవుతోంది...',
    'actions.copy': 'కాపీ',
    'actions.download': 'CSV డౌన్‌లోడ్',
    'actions.pdf': 'PDF ఎగుమతి',
    'actions.email': 'ఇమెయిల్ పంపండి',
    'actions.schedule': 'రిపోర్ట్ షెడ్యూల్ చేయండి',
    'actions.good': 'మంచి సమాధానం',
    'actions.bad': 'చెడ్డ సమాధానం',
    'common.loading': 'లోడ్ అవుతోంది…',
    'common.refresh': 'రిఫ్రెష్',
    'common.save': 'సేవ్',
    'common.cancel': 'రద్దు',
    'common.delete': 'తొలగించు',
    'common.edit': 'సవరించు',
    'language.label': 'భాష',
  },
}

export const LANGUAGES = [
  { code: 'en', label: 'English' },
  { code: 'hi', label: 'हिन्दी' },
  { code: 'te', label: 'తెలుగు' },
]

const I18nContext = createContext({ t: (k) => k, lang: 'en', setLang: () => {} })

function interpolate(str, vars) {
  if (!vars) return str
  return Object.entries(vars).reduce(
    (acc, [k, v]) => acc.replace(new RegExp(`\\{${k}\\}`, 'g'), String(v ?? '')),
    str,
  )
}

export function I18nProvider({ children }) {
  const [lang, setLangState] = useState(() => localStorage.getItem('lang') || 'en')

  useEffect(() => {
    localStorage.setItem('lang', lang)
    document.documentElement.lang = lang
  }, [lang])

  const setLang = useCallback((code) => {
    if (translations[code]) setLangState(code)
  }, [])

  const t = useCallback((key, vars) => {
    const dict = translations[lang] || translations.en
    const raw = dict[key] || translations.en[key] || key
    return interpolate(raw, vars)
  }, [lang])

  return (
    <I18nContext.Provider value={{ t, lang, setLang }}>
      {children}
    </I18nContext.Provider>
  )
}

export function useTranslation() {
  return useContext(I18nContext)
}
