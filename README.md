# Introduction 
perstageutil.duckdb.load

Data_load_utilities are a number of different programs that make data loads in various RDBMS easier.

# What's new

## 1.0
Initial release.


# Getting Started
1.	Installation process

This is homegrown, so I suggest when setting it up on a local workstation, you do that in a Python virtual environment.  This readme just shows just that.

# Virtual envs
To create virtual environments in python....

I suggest opening up command prompt NON-ADMINISTRATIVE and creating a sub directory under you user.  For example, mine is jamoran\pyenvironments.

```{cmd}
md pyenvironments
```

To create the environment.

```{cmd}
python -m venv <environment name in a directory where you want it....>
```

Full example.  Note the environment name and the folder name are the same.s

```{cmd}
python -m venv test_environment
```

I created one locally under pyenvironments\test_environment

Mine...for an example...

```{cmd}
cd C:\Users\jamoran\pyenvironments\test_environment
```

Now activate the environment by running .\Scripts\activate.bat in the test_environment directory.

```{cmd}
.\Scripts\activate.bat
```

You deactive it by running .\Scripts\deactivate.bat.

```{cmd}
.\Scripts\deactivate.bat
```

You can copy the install gzip from our share.

```{cmd}
copy x-1.0.0.0.tar.gz C:\Users\<YOUR AD NAME>\pyenvironments\test_environment\x-1.0.0.0.tar.gz
```

You can then simply install it with pip.

```{cmd}
pip install x-1.0.0.0.tar.gz
```

1. Unistall Process

To uninstall the autodade software.

```{cmd}
pip uninstall hpfc_getinfa
```

To clean up your entire virtual environment you can run the following commands which will remove all installed modules.

```{cmd}
pip freeze > to-uninstall.txt
pip uninstall -r to-uninstall.txt
```

I suggest as well, closing down the cmdshell to clear out any cached values.  Again, make sure you activate your test environment first when re-starting cmdshell.

```{cmd}
exit
```

3.	Software dependencies

The dependencies should be taken care of by the installer.

# Build and Test

## TO BUILD INSTALL ON YOUR COMPUTER

