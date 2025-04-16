import time


class geotabAPI:
    def __init__(self, service_name, client_):
        self.jobId = None
        self._service_name = service_name
        _ = self._create_client(client_)

    def _create_client(self, client_):
        self.client = client_

    def get_data(self, function_name: str, function_parameters: dict) -> dict:
        results = self.client.call(
            "GetAltitudeData",
            serviceName=self._service_name,
            functionName=function_name,
            functionParameters=function_parameters,
        )
        return results

    def create_job(self, params: dict) -> dict:
        results = self.get_data(
            function_name="createQueryJob",
            function_parameters=params,
        )

        self.jobId = results["apiResult"]["results"][0]["id"]
        return results["apiResult"]["results"][0]

    def wait_for_job_to_complete(self, params: dict) -> dict:
        results = self.get_data(
            function_name="getJobStatus",
            function_parameters=params,
        )["apiResult"]

        results = results["results"][0]
        if results and results["status"] and results["status"]["state"] != "DONE":
            time.sleep(5)
            return self.wait_for_job_to_complete(params)
        return results

    def fetch_data(self, params: dict) -> dict:
        index = 1
        while index is not None:
            results = self.get_data(
                function_name="getQueryResults",
                function_parameters=params,
            )["apiResult"]
            error = results["results"][0].get("error", None)
            rows = results["results"][0].get("rows", None)
            page_token = results["results"][0].get("pageToken", None)
            total_rows = results["results"][0].get("totalRows", None)
            params["pageToken"] = page_token
            yield {"data": [rows, total_rows, index], "error": error}
            index += 1
            if not page_token:
                index = None
                yield
