package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class ActionExecutionServiceImpl implements ActionExecutionService {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub client;

    @Override
    public void execute(String hubId, String scenarioName, DeviceActionAvro action) {
        try {
            Instant now = Instant.now();

            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(convert(action))
                    .setTimestamp(timestamp)
                    .build();

            client.handleDeviceAction(request);
        } catch (Exception e) {
            log.error("Ошибка выполнения gRPC запроса для хаба {}: {}", hubId, e.getMessage());
        }
    }

    private DeviceActionProto convert(DeviceActionAvro avro) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(avro.getSensorId())
                .setType(ActionTypeProto.valueOf(avro.getType().name()));

        if (avro.getValue() != null) {
            builder.setValue(avro.getValue());
        }
        return builder.build();
    }
}