# QBK Calendars Suite

Single-service host for all QBK customer calendar views.

## Local Run

```bash
cd qbk-calendars-suite
pip install -r requirements.txt
PORT=8015 python server.py
```

## Routes

- `/daily/`
- `/adult-classes-week/`
- `/adult-dropins-week/`
- `/teen-dropins-week/`
- `/youth-week/`

All routes use the shared DaySmart API endpoint at `/api/events`.
