import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:ndk/entities.dart';
import 'package:ndk/ndk.dart';
import 'package:ndk/shared/nips/nip01/key_pair.dart';

/// Minimal mock relay for testing.
class MockRelay {
  final String name;
  int? _port;
  HttpServer? _server;

  /// Events received by this relay.
  final Set<Nip01Event> storedEvents = {};

  final List<WebSocket> _clients = [];
  final Map<WebSocket, Map<String, List<Filter>>> _subscriptions = {};

  static int _counter = 0;

  MockRelay({required this.name, int? explicitPort}) {
    _port = explicitPort ?? 4040 + (_counter++);
  }

  String get url => 'ws://localhost:$_port';

  Future<void> startServer() async {
    final server = await HttpServer.bind(
      InternetAddress.loopbackIPv4,
      _port!,
      shared: true,
    );
    _server = server;
    server.transform(WebSocketTransformer()).listen((ws) {
      _clients.add(ws);
      _subscriptions[ws] = {};

      ws.listen(
        (message) {
          final jsonMsg = jsonDecode(message as String);
          if (jsonMsg[0] == 'EVENT') {
            final event = Nip01EventModel.fromJson(jsonMsg[1]);
            storedEvents.add(event);
            ws.add(jsonEncode(['OK', event.id, true, '']));
            _broadcastEvent(event);
          } else if (jsonMsg[0] == 'REQ') {
            final subId = jsonMsg[1] as String;
            final filters = <Filter>[];
            for (int i = 2; i < jsonMsg.length; i++) {
              if (jsonMsg[i] is Map<String, dynamic>) {
                try {
                  filters.add(Filter.fromMap(jsonMsg[i]));
                } catch (_) {}
              }
            }
            _subscriptions[ws]?[subId] = filters;
            _respondToRequest(ws, subId, filters);
          } else if (jsonMsg[0] == 'CLOSE') {
            final subId = jsonMsg[1] as String;
            _subscriptions[ws]?.remove(subId);
          }
        },
        onDone: () {
          _clients.remove(ws);
          _subscriptions.remove(ws);
        },
      );
    });
  }

  void _respondToRequest(WebSocket ws, String subId, List<Filter> filters) {
    for (final event in storedEvents) {
      for (final filter in filters) {
        if (_matchesFilter(event, filter)) {
          ws.add(
            jsonEncode([
              'EVENT',
              subId,
              Nip01EventModel.fromEntity(event).toJson(),
            ]),
          );
          break;
        }
      }
    }
    ws.add(jsonEncode(['EOSE', subId]));
  }

  void _broadcastEvent(Nip01Event event) {
    for (final entry in _subscriptions.entries) {
      final ws = entry.key;
      final subs = entry.value;
      for (final subEntry in subs.entries) {
        final subId = subEntry.key;
        final filters = subEntry.value;
        for (final filter in filters) {
          if (_matchesFilter(event, filter)) {
            ws.add(
              jsonEncode([
                'EVENT',
                subId,
                Nip01EventModel.fromEntity(event).toJson(),
              ]),
            );
            break;
          }
        }
      }
    }
  }

  bool _matchesFilter(Nip01Event event, Filter filter) {
    if (filter.kinds != null && !filter.kinds!.contains(event.kind)) {
      return false;
    }
    if (filter.authors != null && !filter.authors!.contains(event.pubKey)) {
      return false;
    }
    if (filter.ids != null && !filter.ids!.contains(event.id)) {
      return false;
    }
    if (filter.since != null && event.createdAt < filter.since!) {
      return false;
    }
    if (filter.until != null && event.createdAt > filter.until!) {
      return false;
    }
    // Check #r tag filter
    final rTags = filter.getTag('r');
    if (rTags != null && rTags.isNotEmpty) {
      final eventRTags = event.tags
          .where((t) => t.isNotEmpty && t[0] == 'r')
          .map((t) => t.length > 1 ? t[1] : '')
          .toList();
      if (!rTags.any((r) => eventRTags.contains(r))) {
        return false;
      }
    }
    return true;
  }

  /// Sends an event to all connected clients.
  void sendEvent({
    required Nip01Event event,
    required String subId,
    KeyPair? keyPair,
  }) {
    for (final ws in _clients) {
      ws.add(
        jsonEncode([
          'EVENT',
          subId,
          Nip01EventModel.fromEntity(event).toJson(),
        ]),
      );
    }
  }

  Future<void> stopServer() async {
    await _server?.close();
    _server = null;
    _clients.clear();
    _subscriptions.clear();
  }
}
