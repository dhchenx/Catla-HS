# CatlaUI
A desktop version built on interfaces of Catla to simply the whole tuning process

## Features
1) Support all commands of Catla in a user-friendly interface using SWT. 
2) For DFO-optimizer, CatlaUI provides a line chart to demonstrate change of time cost over number of interation

## Usage
1) Simply run CatlaUI-run.bat or input 'java -jar CatlaUI.jar' in CMD window. 
2) Choose the project folder and other necessary arguments similar to Catla.
3) All examples are still available when using CatlaUI. 

## Source code and dependencies
The CatlaUI is built on standard Java EE project (NOT Maven). 
Dependencies: 
1) Catla
2) JGoodies-forms
3) JFreeChart
4) SWT

## Main UI

![Screenshot of CatlaUI](../images/catla-ui-screenshot.jpg)

## Known issues
After you run a tuning job, you cannot stop it until you click the CLOSE button to shut down the whole CatlaUI program. 