"""
GPT 분석 서비스
GPT-4o를 사용하여 부고 정보 추출
"""

import json
import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class GPTAnalyzer:
    """GPT-4o 기반 부고 정보 추출기"""

    EXTRACTION_TAGS = [
        '이름', '생년월일', '거주지', '사망일시', '사망장소',
        '장례일정', '장례장소', '발인일시', '화장일시'
    ]

    PROMPT_TEMPLATE = """아래의 <공영장례 정보>에서 [이름, 생년월일, 거주지, 사망일시, 사망장소, 장례일정, 장례장소, 발인일시, 화장일시]을 JSON 형태로 추출해줘. 단, 그외의 사항은 [그 외의 사항]으로 분류해주면 돼.(없는 값은 억지로 찾지마. 그러면 혼난다.)
<공영장례 정보>
{content}"""

    def __init__(self, api_key: str, model: str = "gpt-4o"):
        self.api_key = api_key
        self.model = model
        self.api_url = "https://api.openai.com/v1/chat/completions"

    def analyze(self, content: str) -> Dict[str, Any]:
        """
        부고 내용 분석

        Args:
            content: 스크래핑된 부고 원문

        Returns:
            추출된 정보 딕셔너리
        """
        prompt = self.PROMPT_TEMPLATE.format(content=content.replace(",", "."))

        try:
            response = requests.post(
                self.api_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.0
                },
                timeout=60
            )
            response.raise_for_status()

            data = response.json()
            result_text = data["choices"][0]["message"]["content"]
            result = json.loads(result_text)

            # 키 정규화 (공백 제거)
            normalized = {}
            for key, value in result.items():
                normalized[key.replace(" ", "")] = value

            return normalized

        except requests.exceptions.RequestException as e:
            logger.error(f"GPT API 요청 실패: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"GPT 응답 JSON 파싱 실패: {e}")
            raise
        except (KeyError, IndexError) as e:
            logger.error(f"GPT 응답 구조 오류: {e}")
            raise

    def analyze_raw_data(self, raw_data: Dict[str, str]) -> Dict[str, Any]:
        """
        원본 데이터 분석 (기존 EXEC_PROMPT 대체)

        Args:
            raw_data: {"url": str, "content": str, "updated": int}

        Returns:
            분석 결과 딕셔너리
        """
        logger.debug(f"분석 중: {raw_data['url']}")

        extracted = self.analyze(raw_data["content"])

        return {
            "url": raw_data["url"],
            "updated": raw_data.get("updated", 0),
            "content": extracted
        }


def clean_analyzed_data(data: Dict[str, Any]) -> Dict[str, str]:
    """
    분석된 데이터 정리 (기존 DATA_CLEANER 대체)

    Args:
        data: GPT 분석 결과

    Returns:
        정리된 딕셔너리
    """
    TAGS = [
        '이름', '생년월일', '거주지', '사망일시', '사망장소',
        '장례일정', '장례장소', '발인일시', '화장일시'
    ]

    def convert_value(value: Any) -> str:
        """값 변환"""
        if isinstance(value, dict):
            parts = [f"{k}:{convert_value(v)}" for k, v in value.items()]
            return "\n".join(parts)
        elif isinstance(value, list):
            return ", ".join(str(v) for v in value)
        elif value is None:
            return "추출 실패"
        else:
            return str(value)

    content = data.get("content", {})
    result = {}

    for tag in TAGS:
        value = content.get(tag, "추출 실패")
        converted = convert_value(value)

        # 빈 값 처리
        if converted in ["그 외의 사항", "", "없음", None]:
            converted = "추출 실패"

        result[tag] = converted

    return result
