# Blueprint Dataverse-to-Delta Accelerator
Microsoft Dataverse is a set of data services, tables/schemas and security features that support Dynamics 365 and PowerApps.

## Overview
While there are several integration platforms that perform the task of integrating data from Dataverse to Databricks quite brilliantly (E.g. FiveTran) and who’s approach may offer significant advantages to the following pattern; We built a native Databricks process to showcase a pattern that can be used for not only Dataverse, but any REST ODATA endpoint.

The included notebook is completely dynamic, including the schema inference. It results in a Delta table, ready for analysis.

## Special Thanks
This accelerator was built by Eric Vogelpohl. More details about the motivation for this accelerator is available at [this blog post](https://medium.com/@EricVogelpohl/dataverse-meet-databricks-8f500a388699).
