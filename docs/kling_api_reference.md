# Kling API Reference

**Base URL:** `https://api.klingai.com`  
**Auth:** JWT (HS256) signed with Access Key + Secret Key  
**Last verified:** 2026-04-12

---

## Authentication

Generate a JWT token using your Access Key (AK) and Secret Key (SK):

```python
import jwt, time

def get_kling_token(ak, sk):
    headers = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": ak,                        # Access Key
        "exp": int(time.time()) + 1800,   # 30 min expiry
        "nbf": int(time.time()) - 5       # Clock skew buffer
    }
    return jwt.encode(payload, sk, headers=headers)
```

Usage: `Authorization: Bearer <token>`

---

## Confirmed Active Endpoints

### Video Generation

| Endpoint | Method | Purpose |
|---|---|---|
| `/v1/videos/text2video` | POST | Text-to-video generation |
| `/v1/videos/image2video` | POST | Image-to-video (requires `image`) |
| `/v1/videos/video-extend` | POST | Extend existing video (requires `videoId`) |
| `/v1/videos/lip-sync` | POST | Lip-sync on existing video |
| `/v1/videos/effects` | GET | List available video effects |

### Image Generation

| Endpoint | Method | Purpose |
|---|---|---|
| `/v1/images/generations` | POST | Text/image-to-image generation |
| `/v1/images/kolors-virtual-try-on` | POST | Virtual clothing try-on |

### Task Status Queries

| Endpoint | Method |
|---|---|
| `/v1/videos/text2video/{task_id}` | GET |
| `/v1/videos/image2video/{task_id}` | GET |
| `/v1/videos/lip-sync/{task_id}` | GET |
| `/v1/videos/video-extend/{task_id}` | GET |
| `/v1/images/generations/{task_id}` | GET |
| `/v1/images/kolors-virtual-try-on/{task_id}` | GET |

---

## Video Models (text2video)

| Model | Modes | Notes |
|---|---|---|
| `kling-v1` | `std` | Original model, supports camera control |
| `kling-v1-5` | `pro` only | No `std` mode |
| `kling-v1-6` | `std`, `pro` | Supports camera control |
| `kling-v2-master` | `std`, `pro` | V2 base |
| `kling-v2-1-master` | `std`, `pro` | V2.1 |
| `kling-v2-5-turbo` | `std`, `pro` | Fastest generation |
| `kling-v2-6` | `std`, `pro` | Latest — native audio + motion |

**Invalid video model names:** `kling-v2`, `kling-v2-1`, `kling-v2-5`, `kling-video-o1`

---

## Video Parameters

| Parameter | Type | Values |
|---|---|---|
| `model_name` | string | See table above |
| `prompt` | string | Text description |
| `negative_prompt` | string | What to avoid |
| `duration` | string | `"5"` or `"10"` (seconds only) |
| `aspect_ratio` | string | `"16:9"`, `"9:16"`, `"1:1"` |
| `mode` | string | `"std"` or `"pro"` |
| `cfg_scale` | float | `0.0` to `1.0` (prompt adherence) |
| `camera_control` | object | See below |

### Camera Control

- **Only supported on:** `kling-v1`, `std` mode, `5s` duration
- **Type:** `"simple"` only (`"preset"` and `"custom"` are invalid)
- **Config keys:** `horizontal`, `vertical`, `pan`, `tilt`, `roll`, `zoom` (range `-10` to `10`)

```json
{
  "camera_control": {
    "type": "simple",
    "config": {
      "horizontal": 5,
      "vertical": 0,
      "zoom": -3
    }
  }
}
```

---

## Image Models (generations)

| Model | Notes |
|---|---|
| `kling-v1` | Base model, text-to-image |
| `kling-v1-5` | V1.5, supports `image_reference` |
| `kling-v2` | V2, supports `image_reference` |
| `kling-v2-1` | V2.1 latest, supports `image_reference` |
| `kling-v2-new` | Requires input `image` (image-conditioned only) |

**Invalid image model names:** `kolors`, `kolors-v1`, `kolors-v1-5`

---

## Image Parameters

| Parameter | Type | Values |
|---|---|---|
| `model_name` | string | See table above |
| `prompt` | string | Text description |
| `negative_prompt` | string | What to avoid |
| `n` | int | `1` to `9` (number of outputs) |
| `aspect_ratio` | string | `"16:9"`, `"9:16"`, `"1:1"`, `"4:3"`, `"3:4"`, `"2:3"`, `"3:2"` |
| `image_reference` | string | `"face"` or `"subject"` (v1-5, v2, v2-1, v2-new) |
| `image` | string | URL for reference image |

---

## Lip-Sync

**Endpoint:** `POST /v1/videos/lip-sync`

| Parameter | Type | Values |
|---|---|---|
| `input.mode` | string | `"audio2video"` or `"text2video"` |
| `input.video_id` | string | Required — ID of source video |
| `input.audio_url` | string | For `audio2video` mode |
| `input.text` | string | For `text2video` mode |

---

## Video Extend

**Endpoint:** `POST /v1/videos/video-extend`

| Parameter | Type | Values |
|---|---|---|
| `videoId` | string | Required — ID of video to extend |
| `prompt` | string | Optional continuation prompt |

---

## Virtual Try-On

**Endpoint:** `POST /v1/images/kolors-virtual-try-on`

| Parameter | Type | Values |
|---|---|---|
| `human_image` | string | URL of person image |
| `cloth_image` | string | URL of clothing image |

No `model_name` required.

---

## Async Task Pattern

All generation endpoints are **asynchronous**. The workflow is:

1. **POST** to a generation endpoint → receive a `task_id`
2. **GET** the task status endpoint with `task_id` → poll until `task_status` is `"succeed"` or `"failed"`

### Response Structure

```json
{
  "code": 0,
  "message": "SUCCEED",
  "request_id": "uuid",
  "data": {
    "task_id": "xxx",
    "task_status": "submitted | processing | succeed | failed",
    "task_result": {
      "videos": [{"url": "https://..."}]
    }
  }
}
```

### Error Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1102` | Account balance not enough |
| `1201` | Validation error (check `message` for details) |

---

## Environment Variables

```bash
KLING_ACCESS_KEY=<your_access_key>
KLING_SECRET_KEY=<your_secret_key>
```
