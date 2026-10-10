import os
from dotenv import load_dotenv
from dropbox import DropboxOAuth2FlowNoRedirect

load_dotenv()

flow = DropboxOAuth2FlowNoRedirect(
    os.environ["DROPBOX_APP_KEY"],
    consumer_secret=os.environ["DROPBOX_APP_SECRET"],
    token_access_type="offline",
)

print("Ouvre cette URL et autorise l'application :")
print(flow.start())
code = input("Colle le code d'autorisation affiché par Dropbox : ").strip()
result = flow.finish(code)
print("DROPBOX_REFRESH_TOKEN =", result.refresh_token)