"""Load embedded credentials and app constants."""

from __future__ import annotations

import os

GOOGLE_SHEET_ID = "1eSMeDYvcPdoBw1UYNI7pAnKpCAscBbjxHL-ZJAf879k"
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

RAPIDAPI_KEY = "c2af2cf2b4msh0d63d110f403afbp1233e9jsn8dfa08e0eb1d"
RAPIDAPI_HOST = "fresh-linkedin-scraper-api.p.rapidapi.com"
RAPIDAPI_BASE = f"https://{RAPIDAPI_HOST}/api/v1"

SHEET_PROFILES = "Profiles"
SHEET_ENGAGEMENTS = "Engagements"
SHEET_PROFILES_ENGAGEMENT_UTIL = "Profiles_Engagement_Util"

EMBEDDED_SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "linkedin-auitomation",
    "private_key_id": "c83c7584fa4c46b97ce0764bf188fd6aa1844d12",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDn+BbopJPJn9w3\n0hQwO+WGDg/ufTh87tWrq1GyGOBfYfD1S52PfLEX2nOqmwUSwTSFW2lXBm+BQa3V\nZRiMkfoTLEqzWaUzwGukLGAvPyzUAMYot4KKZKchrjkgOicAlSje9zJJH0DRavCB\njl0QdeUdE4v+2Wv3qG3opE1XEEUHralWYtRLTUIIFSERieKVuuqc9NzmfEWI0MtG\n5t+ZT7+msPx6YKXMW5ANoxyCox2MVmpxdA0CyrqHetohIIQyDUIbaVJJvuPvPR1T\ngh+ctsqsHx1IiPmdDzNDRYtCqf1Wblw3DXnQ6N10A2g7bd7CCnbnoWk4c7U3HRXi\nuKnuIXVNAgMBAAECggEAXOdECY2scOJ4+NRG6KB8humH4Oy+3G5tHhGBqTIEXr8j\nmaJcqrR2WsbPb2Mhr4a4qbZYJJX7v4lV/TK9l8L5JIenLVh4bRciJUDujs2e/xOG\nSZVhj1rLgxY7Y9hWeYnDsjTPq3B8bcMGKuUhCbAmADSta3aZorkOt89h+D4YX3+f\nGJFf/wveG8ciGJSx9bNDnHQlxttFFNuYM9LE3bSR7khSd5UI8l3DFUJhoyPwxoTg\ns9r4U5lOyPJgNmG0RfctFAaBLfdrzbAOGcf6KaqODSCiaMMeI6UWblxTZvSypV90\nPRR0EKAji+pAASpJPxPNDDozBfPUv8RIlGEncF83kwKBgQD7gn2gN6Jil3Q6keNS\n1cONXoX0gCyasjjcunCGh8G6vyvqjKIr7a3UwCxyBeZ0eTdHMCvZDR9DvH0ZRV4V\ncNbTGrhvvy6Q0hwogOY63y3p9/X/jUR3UjYOlH8xXuCTxmBSoK2LP0FXlCs9GRVc\nhxPpy+KTxuPVGP2pasIHwdOpswKBgQDsHEogC1bnsdi3BbY/08/juastF5i+nyEr\niXQkTuxXkyp0WGu9CrZ52lD5rdWYeZaiEzSqUf0JdzGwLEJewzUqTaTNryDBANX0\nmFeIej/VRWZ0UAoaKxlWvfkL2I9KmGNX+/Leden69lymAxaivotmbm09cRDmaFhZ\ns59D3jjk/wKBgQDfJzSXh0VI/OBfZzmvMYNEV227Nk5dI2xYTAOzGZjGPQCWJxls\nqIHnlmrY8Gs9RJ/LRe5hssbersrANU/47hltTPQAEj0auZHKTjP4YDS4tw1JJOpu\nhD76SL9h6rCP7R9hsLbKKeGr9wc2Op89bYw5kHEEdR+I35eRTevCPjOzCQKBgFBc\nqJYThfbCP4K7vPYof5+AuFRWrbRjsQSCejdJbYO5IUAMQE9NcCI1Pk5c7lBBQhXT\n957o3Pj1ysGtrsFWK6hEWQ1wx/Mo96mSmGhpQ4SxZFe+TlHtzWgKrNNtWAgJqfZu\nWJGDDKpQY+RVNMUbmZK5jxDUhO4HIdsWGeUdo7u9AoGBANjvN71IJPgSitVWpVci\nHT5LhMmqLqFJ6kDisxnNpGJ/aA3FO2VvxmKduQO7E5gM/uSLiRaXqqUDw/pmua2B\nVSYu/CYNS+a4JU8nZ0ucaFUeGcqolrC/czw91JZ/+IZTpsrdRxojKf634OFmDVf4\nphbLA8ZPzPBXn52QBh3k6wqz\n-----END PRIVATE KEY-----\n",
    "client_email": "linkedin-profile-scrpaer-linke@linkedin-auitomation.iam.gserviceaccount.com",
    "client_id": "105851923610192609203",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/linkedin-profile-scrpaer-linke%40linkedin-auitomation.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}

def load_credentials() -> tuple[dict, str]:
    """Returns (embedded_google_service_account_dict, rapidapi_key)."""
    g = EMBEDDED_SERVICE_ACCOUNT
    key = (os.environ.get("RAPIDAPI_KEY", "") or RAPIDAPI_KEY).strip()
    if not key:
        raise ValueError("RapidAPI key missing: set RAPIDAPI_KEY.")
    return g, key.strip()
