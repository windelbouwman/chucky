
# Welcome to Chucky

***Firmware updates made child's play!***

Welcome to the chucky project. The goal of the project is to make firmware
updates of small devices faster by the idea that the update will probably
contain mostly the same content as the previous version. The idea for this
was taken from the casync program. casync might not be fit for microcontrollers
where chucky is intended for microcontrollers!

# Structure

- chucky is the program that slices the firmware image in seperate chunks
- tiffany is an implementation of the update algorithm in C which can be run
  on a microcontroller.

# Usage

To compare two firmware files, use the compare command like this:

    python -m chucky compare ../demos/pybv10*.dfu

This example command compares micropython releases from https://micropython.org/download/


# Build status

[![Build Status](https://travis-ci.org/windelbouwman/chucky.svg?branch=master)](https://travis-ci.org/windelbouwman/chucky)


# Ideas

- hash calculation on target is not required, since we keep the table with
  old hashes
- TBD: use leb128 to store integers to save space?

# Credits

- The whole idea began when attending the FOSDEM 2018 talk of Lennart Poettering:
  https://www.youtube.com/watch?v=Hfmpaymmpa8

  https://fosdem.org/2018/schedule/event/distributing_os_images_with_casync/
