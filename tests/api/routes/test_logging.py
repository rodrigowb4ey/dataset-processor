from httpx import AsyncClient


async def test_request_id_header_is_included(client: AsyncClient) -> None:
    response = await client.get("/")

    assert response.status_code == 200
    assert "X-Request-ID" in response.headers
