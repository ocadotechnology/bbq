from src.commons.exceptions import ParameterValidationException


class DatasetIdCreator(object):
    @classmethod
    def create(cls, date, location, project):
        """
        :return:
            Dataset id for specified project and location
                in 'year_week_location_project' format.
            If date, location or project are not specified
                throws ParameterValidationException.
        """
        if date is None:
            raise ParameterValidationException(
                'No date specified, attribute is mandatory.')
        if location is None:
            raise ParameterValidationException(
                'No location specified, attribute is mandatory.')
        if project is None:
            raise ParameterValidationException(
                'No project id specified, attribute is mandatory.')

        year = str(date.year)
        week = format(date.isocalendar()[1], '02')

        return '_'.join((year, week, location, project)).replace('-', '_')
