package io.ray.serve;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica {

  private RayServeReplica deployment;

  @SuppressWarnings("rawtypes")
  public RayServeWrappedReplica(
      String deploymentTag,
      String replicaTag,
      String deploymentDef,
      byte[] initArgsbytes,
      byte[] deploymentConfigBytes,
      String controllerName)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {

    // Parse DeploymentConfig.
    DeploymentConfig deploymentConfig = ServeProtoUtil.parseDeploymentConfig(deploymentConfigBytes);

    // Parse init args.
    Object[] initArgs = parseInitArgs(initArgsbytes, deploymentConfig);

    // Instantiate the object defined by deploymentDef.
    Class deploymentClass = Class.forName(deploymentDef);
    Object callable = ReflectUtil.getConstructor(deploymentClass, initArgs).newInstance(initArgs);

    // Get the controller by controllerName.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(controllerName), "Must provide a valid controllerName");
    Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
    Preconditions.checkState(optional.isPresent(), "Controller does not exist");

    // Set the controller name so that Serve.connect() in the user's deployment code will connect to
    // the instance that this deployment is running in.
    Serve.setInternalReplicaContext(deploymentTag, replicaTag, controllerName, callable);

    // Construct worker replica.
    deployment = new RayServeReplica(callable, deploymentConfig, optional.get());
  }

  private Object[] parseInitArgs(byte[] initArgsbytes, DeploymentConfig deploymentConfig)
      throws IOException {

    if (initArgsbytes == null || initArgsbytes.length == 0) {
      return new Object[0];
    }

    if (!deploymentConfig.getIsCrossLanguage()) {
      // If the construction request is from Java API, deserialize initArgsbytes to Object[]
      // directly.
      return MessagePackSerializer.decode(initArgsbytes, Object[].class);
    } else {
      // For other language like Python API, not support Array type.
      return new Object[] {MessagePackSerializer.decode(initArgsbytes, Object.class)};
    }
  }

  /**
   * The entry method to process the request.
   *
   * @param requestMetadata the real type is byte[] if this invocation is cross-language. Otherwise,
   *     the real type is {@link io.ray.serve.generated.RequestMetadata}.
   * @param requestArgs The input parameters of the specified method of the object defined by
   *     deploymentDef. The real type is serialized {@link io.ray.serve.generated.RequestWrapper} if
   *     this invocation is cross-language. Otherwise, the real type is Object[].
   * @return the result of request being processed
   * @throws InvalidProtocolBufferException if the protobuf deserialization fails.
   */
  public Object handleRequest(Object requestMetadata, Object requestArgs)
      throws InvalidProtocolBufferException {
    boolean isCrossLanguage = requestMetadata instanceof byte[];
    return deployment.handleRequest(
        new Query(
            isCrossLanguage
                ? ServeProtoUtil.parseRequestMetadata((byte[]) requestMetadata)
                : (RequestMetadata) requestMetadata,
            isCrossLanguage
                ? ServeProtoUtil.parseRequestWrapper((byte[]) requestArgs)
                : requestArgs));
  }

  /** Check whether this replica is ready or not. */
  public void ready() {
    return;
  }

  /** Wait until there is no request in processing. It is used for stopping replica gracefully. */
  public void drainPendingQueries() {
    deployment.drainPendingQueries();
  }
}
