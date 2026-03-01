from google_auth_oauthlib.flow import InstalledAppFlow
import pickle

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

flow = InstalledAppFlow.from_client_secrets_file(
    'client_secret_139513792704-ob42dblstj2dco839ggmenj37loj06a6.apps.googleusercontent.com.json', SCOPES
)
creds = flow.run_local_server(port=0)

with open('token.pkl', 'wb') as f:
    pickle.dump(creds, f)
print("Sucuessful")