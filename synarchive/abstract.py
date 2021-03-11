#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import abc

# Libs


# Custom


##################
# Configurations #
##################


############################################
# Archive Abstract Class - AbstractRecords #
############################################

class AbstractRecords(abc.ABC):

    @abc.abstractmethod
    def create(self):
        """ Creates a new record in a specified subject table within database
        """
        pass


    @abc.abstractmethod
    def read_all(self):
        """ Retrieves all records in a specified table of the database
        """
        pass
    

    @abc.abstractmethod
    def read(self):
        """ Retrieves a single record from a specified table in the database
        """
        pass


    @abc.abstractmethod
    def update(self):
        """ Updates an existing record with specified updates
        """
        pass


    @abc.abstractmethod
    def delete(self):
        """ Deletes a specified record from the specified table in the database
        """
        pass