# Example framework

A simple Mesos framework that demonstrates API client usage.

## Running

You'll need a Mesos master accessible at `$mesos_uri`.

From the project root directory run the following commands:

```bash
mvn clean package
package_path="mesos-rxjava-example/mesos-rxjava-example-framework/target"
package_file="mesos-rxjava-example-framework-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
main_class="com.mesosphere.mesos.rx.java.example.framework.sleepy.Sleepy"
mesos_uri="http://localhost:5050/api/v1/scheduler"
cpus_per_task="0.04"
mesos_resource_role="*"
java -cp "$package_path/$package_file" "$main_class" "$mesos_uri" "$cpus_per_task" "$mesos_resource_role"
```
