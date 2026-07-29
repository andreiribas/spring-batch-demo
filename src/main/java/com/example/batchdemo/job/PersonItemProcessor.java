package com.example.batchdemo.job;

import com.example.batchdemo.domain.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;

/**
 * Basic feature: ItemProcessor - transforms each item between read and write.
 * Returning null here would filter the item out of the chunk entirely.
 */
public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public Person process(Person person) {
        String firstName = person.getFirstName().trim().toUpperCase();
        String lastName = person.getLastName().trim().toUpperCase();

        Person transformed = new Person(firstName, lastName);
        log.info("Converting '{}' into '{}'", person, transformed);
        return transformed;
    }
}
