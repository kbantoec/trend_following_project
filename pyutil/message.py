import logging

import requests
import os


class Mail(object):
    """
    Class for sending emails with and without attachments via mailgun
    """

    def __init__(self, mailgunapi=None, mailgunkey=None, to_adr=None, from_adr=None, text="", subject=""):
        """
        Create a Mail object
        """
        # make sure that mailgun is of the correct type as specified in the config
        self.__mailgun_api = mailgunapi or os.environ["MAILGUNAPI"]
        self.__mailgun_key = mailgunkey or os.environ["MAILGUNKEY"]
        self.__files = list()
        self.__subject = subject
        self.__text = text
        self.__toAdr = to_adr
        self.__fromAdr = from_adr

    def attach_file(self, name, localpath, mode="r+b"):
        """
        attach a file.

        :param name:
        :param localpath:
        :param mode:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("attachment", (name, open(localpath, mode=mode)))])
        return self

    def attach_stream(self, name, stream):
        """
        attach a stream (or string).

        :param name:
        :param frame:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("attachment", (name, stream))])
        return self

    def inline_file(self, name, localpath, mode="r+b"):
        """
        inline a file.

        :param name:
        :param localpath:
        :param mode:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("inline", (name, open(localpath, mode=mode)))])
        return self

    def inline_stream(self, name, stream):
        """
        inline a file.

        :param name:
        :param stream:

        :return: reference to modified object to chain commands
        """
        self.__files.extend([("inline", (name, stream))])
        return self

    def send(self, html=None, logger=None):
        """
        send an email
        """
        logger = logger or logging.getLogger(__name__)
        assert self.text != "" or html, "Specify either some text or some html!"

        try:
            data = {"from": self.fromAdr, "to": self.toAdr, "subject": self.subject, "text": self.text}
            if html:
                data["html"] = html

            for file in self.__files:
                logger.info("type: {0}, name: {1}".format(file[0], file[1][0]))

            r = requests.post(self.__mailgun_api, auth=("api", self.__mailgun_key), files=self.__files, data=data)
            assert r.ok, "Something went wront sending the email"

        finally:

            for f in self.__files:
                # List of tuples ("attachment or inline", (f[0], f[1]))
                try:
                    f[1][1].close()
                except:
                    pass

    @property
    def text(self):
        return self.__text


    @property
    def toAdr(self):
        return self.__toAdr

    @property
    def fromAdr(self):
        return self.__fromAdr

    @property
    def subject(self):
        return self.__subject

    @text.setter
    def text(self, value):
        self.__text = value

    @subject.setter
    def subject(self, value):
        self.__subject = value

    @fromAdr.setter
    def fromAdr(self, value):
        self.__fromAdr = value

    @toAdr.setter
    def toAdr(self, value):
        self.__toAdr = value

    @property
    def files(self):
        # List of tuples ("attachment or inline", (f[0], f[1]))
        return self.__files
