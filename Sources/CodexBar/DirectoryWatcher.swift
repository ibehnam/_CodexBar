import Foundation

/// Watches directories for file system changes using FSEvents.
/// Triggers a callback when any file in the watched directories is modified.
final class DirectoryWatcher: @unchecked Sendable {
    private var stream: FSEventStreamRef?
    private let callback: @Sendable () -> Void
    private let directories: [String]
    private let debounceInterval: TimeInterval
    private var debounceTask: Task<Void, Never>?
    private let lock = NSLock()

    /// Creates a directory watcher.
    /// - Parameters:
    ///   - directories: Directories to watch for changes.
    ///   - debounceInterval: Minimum time between callbacks (to avoid rapid-fire updates).
    ///   - callback: Called when a change is detected (after debounce).
    init(directories: [String], debounceInterval: TimeInterval = 1.0, callback: @escaping @Sendable () -> Void) {
        self.directories = directories.filter { FileManager.default.fileExists(atPath: $0) }
        self.debounceInterval = debounceInterval
        self.callback = callback
    }

    func start() {
        lock.lock()
        defer { lock.unlock() }

        guard stream == nil, !directories.isEmpty else { return }

        var context = FSEventStreamContext()
        context.info = Unmanaged.passUnretained(self).toOpaque()

        let paths = directories as CFArray
        let flags = FSEventStreamCreateFlags(
            kFSEventStreamCreateFlagUseCFTypes |
            kFSEventStreamCreateFlagFileEvents |
            kFSEventStreamCreateFlagNoDefer
        )

        guard let stream = FSEventStreamCreate(
            nil,
            { _, info, numEvents, _, _, _ in
                guard let info, numEvents > 0 else { return }
                let watcher = Unmanaged<DirectoryWatcher>.fromOpaque(info).takeUnretainedValue()
                watcher.handleEvents()
            },
            &context,
            paths,
            FSEventStreamEventId(kFSEventStreamEventIdSinceNow),
            0.5, // Latency in seconds before delivering events
            flags
        ) else { return }

        self.stream = stream
        FSEventStreamSetDispatchQueue(stream, DispatchQueue.main)
        FSEventStreamStart(stream)
    }

    func stop() {
        lock.lock()
        defer { lock.unlock() }

        debounceTask?.cancel()
        debounceTask = nil

        guard let stream else { return }
        FSEventStreamStop(stream)
        FSEventStreamInvalidate(stream)
        FSEventStreamRelease(stream)
        self.stream = nil
    }

    private func handleEvents() {
        lock.lock()
        debounceTask?.cancel()
        let interval = debounceInterval
        let cb = callback
        debounceTask = Task {
            try? await Task.sleep(for: .seconds(interval))
            guard !Task.isCancelled else { return }
            cb()
        }
        lock.unlock()
    }

    deinit {
        // Safety fallback - stop should be called before deinit
        debounceTask?.cancel()
        if let stream {
            FSEventStreamStop(stream)
            FSEventStreamInvalidate(stream)
            FSEventStreamRelease(stream)
        }
    }
}
