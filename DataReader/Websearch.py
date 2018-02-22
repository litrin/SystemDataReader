from base import RawDataFileReader
from xml.etree import ElementTree


class WebSearchResult(RawDataFileReader):
    """
    Helper to reade web search output logs.
    """
    xml_body = None

    def __init__(self, path, xml_only=False):
        """
        :param path: abs file name
        :param xml_only: bool, set True if the file is a pure xml file
        """
        self.filename = path
        content = self.reader()

        if xml_only is False:
            content = content[31:]

        self.xml_body = ElementTree.fromstringlist(content)[1]

    @property
    def ops(self):
        return float(self.xml_body.find("metric").text)

    @property
    def user(self):
        return int(self.xml_body.find("users").text)

    @property
    def rt(self):
        rt_element = self.xml_body.find("responseTimes")[0]
        return float(rt_element.find("avg").text)

    @property
    def rt_detail(self):
        rt_element = self.xml_body.find("responseTimes")[0]

        return {
            key: float(rt_element.find(key).text) for key in
            ["avg", "max", "sd"]
        }

    @property
    def delay(self):
        rt_element = self.xml_body.find("delayTimes")[0]
        return float(rt_element.find("actualAvg").text)

    @property
    def delay_detail(self):
        rt_element = self.xml_body.find("delayTimes")[0]
        return {
            key: float(rt_element.find(key).text) for key in
            ["min", "max", "actualAvg", "targetedAvg"]
        }
