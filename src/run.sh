#!/bin/bash
sleep 5 && python postgres_ddl.py && python schedule.py && python players.py