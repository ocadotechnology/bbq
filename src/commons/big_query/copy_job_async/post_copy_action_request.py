class PostCopyActionRequest(object):

    def __init__(self, url, data):
        self.__url = url
        self.__data = data

    @property
    def url(self):
        return self.__url

    @property
    def data(self):
        return self.__data

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "URL: {}, with data: {}".format(self.__url, self.__data)

    def __eq__(self, o):
        return type(o) is PostCopyActionRequest \
               and self.__url == o.__url \
               and self.__data == o.__data

    def __ne__(self, other):
        return not (self == other)

    def to_json(self):
        return dict(url=self.__url, data=self.__data)

    @classmethod
    def from_json(cls, json):
        return PostCopyActionRequest(url=json["url"], data=json["data"])
