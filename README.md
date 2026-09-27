# BingoLink Telegram File Store Bot — MongoDB Edition

This is the **complete Telegram bot project (7 files)**. The BingoLink Next.js website is **unchanged** and can continue using PostgreSQL on Aiven. Only this new Telegram bot stores its data in **MongoDB**. Videos stay in a private Telegram backup channel; MongoDB stores video references, bundles, one-time unlock hashes, force-join events, and bot settings.

## What the bot does

- Owner or sudo uploads a video: backup-channel message is saved and a share link is generated.
- `/bulk` + several videos + `/done` creates one link for all videos (up to 30 per bundle).
- A visitor opens the bot share link, passes force-join checks, receives a new BingoLink short link, completes your site's independent article flow, and is returned to the bot using a user-bound, **one-time token**.
- One successful redemption sends the package videos. Incomplete deliveries can be resumed by the same user without repeating the article flow.
- `/settings` controls backup channel, forwarding protection, auto-delete, mandatory chats, and sudo uploaders. Private join-request events received by the bot within 24 hours can count as joined, as previously requested; a request not received by the bot cannot be checked.
- MongoDB creates the necessary collections and unique indexes automatically at launch; no SQL file or Aiven connection is needed for this bot.

## Install / deploy on Railway

1. Create a MongoDB Atlas cluster and a **dedicated database user** with read/write access to just the `bingolink_filebot` database. Allow Railway's network access in Atlas. Prefer a fixed egress IP where supported; review IP allowlist exposure. Keep MongoDB authentication and TLS enabled.
2. Create a private Telegram backup channel; make your bot an admin with **Post Messages** permission. For force-join channels/groups, bot must also be admin. For private join-request verification, it needs **Invite Users** permission.
3. Create a dedicated BingoLink bot-user account and developer API key. This is a BingoLink key, **not** a Telegram or ad-network token.
4. Upload these seven source files to a **separate** GitHub repository; the existing BingoLink website repository is unaffected. Create a Railway service from that repository using Dockerfile deployment. **Only one instance/replica** should long-poll with a Telegram token.
5. Set private Railway environment variables as shown below and deploy. No public HTTP port/domain is needed.
6. Look for `MongoDB collections initialized` in the deployment logs. Open the bot, send `/settings`, use `/setbackup -100...`, send a test video, and try the full shortener + one-time unlock journey.

| Variable | Description |
|---|---|
| `BOT_TOKEN` | New Telegram bot token from @BotFather |
| `BOT_OWNER_ID` | Your numeric Telegram user ID (send `/id` to the bot) |
| `MONGODB_URI` | Atlas `mongodb+srv://...` **private connection string** with URL-encoded password; TLS enabled |
| `MONGODB_DB` | `bingolink_filebot` (or another dedicated bot database name) |
| `BINGOLINK_API_KEY` | `bl_live_...` API key of the dedicated bot-user account on BingoLink |
| `BINGOLINK_BASE_URL` | `https://www.bingolink.site` |
| `BACKUP_CHANNEL_ID` | Optional private backup channel ID, or set via `/setbackup` |

Use `.env.example` for variable names only. Never commit an actual `.env` file, passwords, tokens, or connection strings. Rotate any credential previously shared in a chat.

## Important: existing PostgreSQL bot data

**This MongoDB edition does not migrate the previous bot's PostgreSQL tables.** A brand-new MongoDB database starts with no saved bot video metadata, sudo list, or force-join settings. If you already launched the PostgreSQL edition and need old packages, migrate those records separately before switching. Telegram backup videos remain in the channel, but the new bot cannot find them until their metadata is imported. Don't run both editions simultaneously using the same Telegram token.

## Bot commands

`/start`, `/id`, `/help` — general commands. Owner and sudo: send video, `/bulk`, `/done`, `/cancel`, `/files`. Owner: `/settings`, `/stats`, `/setbackup -100...`, `/addjoin @name public`, `/addjoin -100... private`, `/rmjoin -100...`, `/addsudo USER_ID`, `/remsudo USER_ID`, `/setdelete MINUTES`, `/forward on|off`, `/forcejoin on|off`.

## Security and limitations

- Tokens are hashed in MongoDB, tied to a Telegram user, expire after three hours, and use an **atomic** claim to stop two redemptions from both becoming first-use. MongoDB Atlas replica-set transactions protect multi-record bulk changes.
- Telegram polling, local per-token delivery locks, and media cleanups are configured for **one Railway replica**, not multi-replica operation. Restart halfway through delivery may require manual recovery in rare cases.
- Bot cannot prove every client viewed the ads; the BingoLink website independently enforces its own article steps. It must not require users to click ads.
- Automatic DM deletion is best-effort and subject to Telegram restrictions (usually within 48 hours); protected content is not copy-proof.
- MongoDB is only for the new bot. **Do not remove `DATABASE_URL` from the BingoLink website**, which still needs its existing PostgreSQL database.

The Python source has been syntax checked and offline logic tests are included in the release process, but live Telegram, MongoDB Atlas, and BingoLink integration were not available in this environment. Deploy a test bot before switching your production bot.


## Compact, grouped bot settings
Open `/settings` as the owner. Six submenus: Start Message, Link Message, Branding, Files & Backup, Force Join, and Sudo. The photo and caption of both public messages, Premium/Tutorial URLs, custom button labels, Admin Contact and Powered By text are editable through inline menus. Reply with a photo or text after tapping its field; `/cancel` aborts. Photos use Telegram file IDs (no external storage required). Values are saved in MongoDB's existing settings collection; no migrations. Start HELP/CLOSE and link PREMIUM/TUTORIAL buttons can each be toggled. Premium and tutorial buttons only show after a valid HTTPS URL has been saved. Use Preview to check both layouts. The bot retains the one-time BingoLink unlock flow and existing `/bulk`, force-join, sudo and auto-delete commands.
