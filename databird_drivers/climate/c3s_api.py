import requests
import time
import threading


class C3SApiError(Exception):
    pass


class Task:
    def __init__(self, response):
        self.response = response
        self.reply = self._check_response(response)

    @staticmethod
    def _check_response(response):
        """Either returns a dict or raises an error."""
        response.raise_for_status()
        try:
            reply = response.json()
        except:
            raise C3SApiError(reply.text)

        if "message" in reply:
            error = reply["message"]
            if context in reply:
                error += str(context)
            raise C3SApiError(error)

        return reply

    def download(self, target_file):
        if not self.is_completed:
            raise C3SApiError("Request not completed, state is: " + self.state)
        url = self.reply["location"]
        size = self.reply["content_length"]
        content_type = self.reply["content_type"]
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(target_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        return size, content_type

    @property
    def state(self):
        return self.reply["state"]

    @property
    def request_id(self):
        return self.reply["request_id"]

    @property
    def is_completed(self):
        return self.state == "completed"

    @property
    def is_queued(self):
        return self.state == "queued"

    @property
    def is_running(self):
        return self.state == "running"

    @property
    def is_failed(self):
        return self.state == "failed"


class Client:
    def __init__(self, url, uid, key):
        self.url = url.rstrip("/")
        self.uid = uid
        self.key = key

        self.session = requests.Session()
        self.session.auth = requests.auth.HTTPBasicAuth(uid, key)

    def submit(self, name, request):
        url = "{}/resources/{}".format(self.url, name)
        r = self.session.post(url, json=request)
        return Task(r)

    def status(self, request_id):
        url = "{}/tasks/{}".format(self.url, request_id)
        r = self.session.get(url)
        return Task(r)

    def download(self, request_id, target_file):
        r = self.status(request_id)
        return r.download(target_file)

    def wait(self, request_id, verbose=False, timeout=None):
        """Wait for completion of task."""
        delay = 1
        max_delay = 120
        start = time.time()
        last_status = None
        while True:
            t = self.status(request_id)
            if verbose and last_status != t.state:
                last_status = t.state
                print("Request {} status is {}".format(t.request_id, t.state))
            if t.is_completed:
                return t
            if t.is_failed:
                return t
            if t.is_queued or t.is_running:
                # do nothing but wait
                time.sleep(delay)
                delay *= 1.5
                if delay > max_delay:
                    delay = max_delay
            if timeout is not None:
                if time.time() - start >= timeout:
                    raise TimeoutError(str(request_id))
