"""Hänvisningsmeddelanden för exkluderade rättsområden."""
REFERRALS = {
    "straffrätt": "Kontakta en advokat eller Rättshjälpen: https://www.rattshjalpen.se",
    "asyl": "Kontakta Advokatjouren eller Rådgivningsbyrån: https://www.sweref.org",
    "skatterätt": "Kontakta Skatteverket: https://www.skatteverket.se",
    "vbu": "Kontakta familjerätten i din kommun.",
}

def get_referral(area_id: str) -> str:
    return REFERRALS.get(area_id, "Kontakta en jurist för hjälp med denna fråga.")
